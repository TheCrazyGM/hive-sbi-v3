import json
import time
from datetime import datetime, timezone

from nectar import Hive
from nectar.account import Account
from nectar.amount import Amount
from nectar.nodelist import NodeList
from nectar.utils import formatTimeString

from hive_sbi.hsbi.core import get_logger
from hive_sbi.hsbi.transfer_ops_storage import AccountTrx, TransferTrx

logger = get_logger()


def get_account_trx_data(account, start_block, start_index) -> list:
    """
    Retrieve all transfer operations for a given account from a starting block and index.
    Returns a list of transaction dictionaries.
    """
    # Go through all transfer ops
    if start_block is not None:
        trx_in_block = start_block["trx_in_block"]
        op_in_trx = start_block["op_in_trx"]
        virtual_op = start_block["virtual_op"]
        start_block = start_block["block"]

        logger.info(f"account {account['name']} - {start_block}")
    else:
        start_block = 0
        trx_in_block = 0
        op_in_trx = 0
        virtual_op = 0

    if start_index is not None:
        # Check if start_index is a dictionary or an integer
        if isinstance(start_index, dict) and "op_acc_index" in start_index:
            start_index = start_index["op_acc_index"] + 1
        # If it's already an integer, we can use it directly
        # print("account %s - %d" % (account["name"], start_index))
    else:
        start_index = 0

    data = []
    last_block = 0
    last_trx = trx_in_block
    for op in account.history(start=start_block - 5, use_block_num=True):
        if op["block"] < start_block:
            # last_block = op["block"]
            continue
        elif op["block"] == start_block:
            if op["virtual_op"] == 0:
                if op["trx_in_block"] < trx_in_block:
                    last_trx = op["trx_in_block"]
                    continue
                if op["op_in_trx"] <= op_in_trx and (trx_in_block != last_trx or last_block == 0):
                    continue
            else:
                if op["virtual_op"] <= virtual_op and (trx_in_block == last_trx):
                    continue
        start_block = op["block"]
        virtual_op = op["virtual_op"]
        trx_in_block = op["trx_in_block"]

        if trx_in_block != last_trx or op["block"] != last_block:
            op_in_trx = op["op_in_trx"]
        else:
            op_in_trx += 1
        if virtual_op > 0:
            op_in_trx = 0
            if trx_in_block > 255:
                trx_in_block = 0

        d = {
            "block": op["block"],
            "op_acc_index": start_index,
            "op_acc_name": account["name"],
            "trx_in_block": trx_in_block,
            "op_in_trx": op_in_trx,
            "virtual_op": virtual_op,
            "timestamp": formatTimeString(op["timestamp"]),
            "type": op["type"],
            "op_dict": json.dumps(op),
        }
        # op_in_trx += 1
        start_index += 1
        last_block = op["block"]
        last_trx = trx_in_block
        data.append(d)
    return data


def get_account_trx_storage_data(account, start_index, hv) -> list:
    """
    Retrieve all 'transfer' operations for an account from a starting index.
    Returns a list of transaction storage dictionaries.
    """
    if start_index is not None:
        start_index = start_index["op_acc_index"] + 1
        logger.info(f"account {account['name']} - {start_index}")

    data = []
    for op in account.history(start=start_index, use_block_num=False, only_ops=["transfer"]):
        amount = Amount(op["amount"], blockchain_instance=hv)
        virtual_op = op["virtual_op"]
        trx_in_block = op["trx_in_block"]
        if virtual_op > 0:
            trx_in_block = -1
        memo = ascii(op["memo"])
        d = {
            "block": op["block"],
            "op_acc_index": op["index"],
            "op_acc_name": account["name"],
            "trx_in_block": trx_in_block,
            "op_in_trx": op["op_in_trx"],
            "virtual_op": virtual_op,
            "timestamp": formatTimeString(op["timestamp"]),
            "from": op["from"],
            "to": op["to"],
            "amount": amount.amount,
            "amount_symbol": amount.symbol,
            "memo": memo,
            "op_type": op["type"],
        }
        data.append(d)
    return data


def _batch_insert(add_batch_func, data: list, batch_size: int = 1000) -> None:
    """
    Helper to batch-insert data using the provided add_batch_func.
    """
    data_batch = []
    for cnt, item in enumerate(data, 1):
        data_batch.append(item)
        if cnt % batch_size == 0:
            add_batch_func(data_batch)
            data_batch = []
    if data_batch:
        add_batch_func(data_batch)


def run():
    from hive_sbi.hsbi.core import load_config, setup_database_connections, setup_storage_objects
    from hive_sbi.hsbi.utils import measure_execution_time

    # Initialize start time for measuring execution time
    start_prep_time = time.time()

    # Load configuration
    config_data = load_config()

    # Setup database connections
    db, db2 = setup_database_connections(config_data)

    # Setup storage objects
    storage = setup_storage_objects(db, db2)

    # Get accounts directly from storage
    accounts = storage["accounts"]
    other_accounts = storage["other_accounts"]

    # Get hive_blockchain setting
    hive_blockchain = config_data.get("hive_blockchain", True)

    # Get configuration directly from storage
    conf_setup = storage["conf_setup"]
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]

    # Ensure last_cycle is always UTC-aware (defensive fix for legacy or bad DB data)
    if last_cycle is not None:
        if last_cycle.tzinfo is None:
            logger.warning("last_cycle is not timezone-aware. Forcing UTC.")
            last_cycle = last_cycle.replace(tzinfo=timezone.utc)
        minutes_since_last_cycle = (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60
        logger.info(
            f"sbi_store_ops_db: last_cycle: {formatTimeString(last_cycle)} - {minutes_since_last_cycle:.2f} min"
        )

    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min
    ):
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes()
        # nodes.update_nodes(weights={"hist": 1})
        hv = Hive(node=nodes.get_nodes(hive=hive_blockchain))
        logger.info(str(hv))

        logger.info("Fetch new account history ops.")

        # Blockchain instance is not needed here

        accountTrx = {}
        for account in accounts:
            if account == "steembasicincome":
                accountTrx["sbi"] = AccountTrx(db, "sbi")
            else:
                accountTrx[account] = AccountTrx(db, account)

        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")

        for account_name in accounts:
            if account_name == "steembasicincome":
                account = Account(account_name, blockchain_instance=hv)
                account_name = "sbi"
            else:
                account = Account(account_name, blockchain_instance=hv)
            start_block = accountTrx[account_name].get_latest_block()
            start_index = (
                accountTrx[account_name].get_latest_index() if start_block is not None else 0
            )

            data = get_account_trx_data(account, start_block, start_index)

            # Process data using the same approach as the original code
            # Batch insert account transaction data
            _batch_insert(accountTrx[account_name].add_batch, data, batch_size=1000)

        # Process other accounts using a new TransferTrx instance
        transferTrxStorage = TransferTrx(db)

        for account in other_accounts:
            account = Account(account, blockchain_instance=hv)
            start_index = transferTrxStorage.get_latest_index(account["name"])

            data = get_account_trx_storage_data(account, start_index, hv)

            # Process data using the same approach as the original code
            # Batch insert transfer transaction storage data
            _batch_insert(transferTrxStorage.add_batch, data, batch_size=1000)
        logger.info(f"store_ops_db script run {measure_execution_time(start_prep_time):.2f} s")


if __name__ == "__main__":
    run()
