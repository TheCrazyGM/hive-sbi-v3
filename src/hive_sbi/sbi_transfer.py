import json
import time
from datetime import datetime, timezone

from nectar import Hive
from nectar.account import Account
from nectar.amount import Amount
from nectar.nodelist import NodeList
from nectar.utils import formatTimeString

from hive_sbi.hsbi.member import Member
from hive_sbi.hsbi.parse_hist_op import ParseAccountHist
from hive_sbi.hsbi.storage import TransactionOutDB
from hive_sbi.hsbi.transfer_ops_storage import AccountTrx


def run():
    from hive_sbi.hsbi.core import load_config, setup_database_connections, setup_storage_objects
    from hive_sbi.hsbi.utils import measure_execution_time

    start_prep_time = time.time()

    # Load configuration
    config_data = load_config()

    # Setup database connections
    db, db2 = setup_database_connections(config_data)

    # Setup storage objects
    storage = setup_storage_objects(db, db2)

    # Get accounts
    accounts = storage["accounts"]
    other_accounts = storage["other_accounts"]

    # Get management shares and blockchain setting
    mgnt_shares = config_data.get("mgnt_shares", {})
    hive_blockchain = config_data.get("hive_blockchain", True)

    # Setup account transaction storage
    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)

    # Get storage objects
    trxStorage = storage["trxStorage"]
    memberStorage = storage["memberStorage"]
    keyStorage = storage["keyStorage"]
    transactionStorage = storage["transactionStorage"]
    # Create TransactionOutDB instance as it's not available in the storage dictionary
    transactionOutStorage = TransactionOutDB(db)

    # Get configuration
    conf_setup = storage["conf_setup"]
    last_cycle = conf_setup["last_cycle"]
    # Add timezone information to last_cycle if it's offset-naive
    if last_cycle is not None and last_cycle.tzinfo is None:
        from nectar.utils import addTzInfo

        last_cycle = addTzInfo(last_cycle)
    share_cycle_min = conf_setup["share_cycle_min"]

    print(
        "sbi_transfer: last_cycle: %s - %.2f min"
        % (
            formatTimeString(last_cycle),
            (datetime.now(timezone.utc) - last_cycle).total_seconds() / 60,
        )
    )
    confStorage = storage.get("confStorage")

    if (
        last_cycle is not None
        and (datetime.now(timezone.utc) - last_cycle).total_seconds() > 60 * share_cycle_min
    ):
        # ... main processing logic ...
        confStorage.update({"last_cycle": datetime.now(timezone.utc)})
        key_list = []
        print("Parse new transfers.")
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        # print(key_list)
        nodes = NodeList()
        try:
            nodes.update_nodes()
        except Exception as e:
            print(f"could not update nodes: {str(e)}")
        hv = Hive(keys=key_list, node=nodes.get_nodes(hive=hive_blockchain))
        # set_shared_blockchain_instance(hv)

        # print("load member database")
        member_accounts = memberStorage.get_all_accounts()
        member_data = {}
        n_records = 0
        share_age_member = {}
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))

        if True:
            print("delete from transaction_memo... ")
            transactionStorage.delete_sender("dtube.rewards")
            transactionStorage.delete_sender("reward.app")
            transactionStorage.delete_to("sbi2")
            transactionStorage.delete_to("sbi3")
            transactionStorage.delete_to("sbi4")
            transactionStorage.delete_to("sbi5")
            transactionStorage.delete_to("sbi6")
            transactionStorage.delete_to("sbi7")
            transactionStorage.delete_to("sbi8")
            transactionStorage.delete_to("sbi9")
            transactionStorage.delete_to("sbi10")
            print("done.")

        stop_index = None
        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")

        for account_name in accounts:
            if account_name == "steembasicincome":
                account_trx_name = "sbi"
            else:
                account_trx_name = account_name
            parse_vesting = account_name == "steembasicincome"
            accountTrx[account_trx_name].db = db
            account = Account(account_name, blockchain_instance=hv)
            # print(account["name"])
            pah = ParseAccountHist(
                account,
                "",
                trxStorage,
                transactionStorage,
                transactionOutStorage,
                member_data,
                memberStorage=memberStorage,
                blockchain_instance=hv,
            )

            op_index = trxStorage.get_all_op_index(account["name"])

            if len(op_index) == 0:
                start_index = 0
                op_counter = 0
                start_index_offset = 0
            else:
                op = trxStorage.get(op_index[-1], account["name"])
                start_index = op["index"] + 1
                op_counter = op_index[-1] + 1
                if account_name == "steembasicincome":
                    start_index_offset = 316
                else:
                    start_index_offset = 0

            # print("start_index %d" % start_index)
            # ops = []
            #

            ops = accountTrx[account_trx_name].get_all(
                op_types=["transfer", "delegate_vesting_shares"]
            )
            if len(ops) == 0:
                continue

            if ops[-1]["op_acc_index"] < start_index - start_index_offset:
                continue
            for op in ops:
                if op["op_acc_index"] < start_index - start_index_offset:
                    continue
                if stop_index is not None and formatTimeString(op["timestamp"]) > stop_index:
                    continue
                json_op = json.loads(op["op_dict"])
                json_op["index"] = op["op_acc_index"] + start_index_offset
                if account_name != "steembasicincome" and json_op["type"] == "transfer":
                    if float(Amount(json_op["amount"], blockchain_instance=hv)) < 1:
                        continue
                    if json_op["memo"][:8] == "https://":
                        continue

                pah.parse_op(json_op, parse_vesting=parse_vesting)

        print(f"transfer script run {measure_execution_time(start_prep_time):.2f} s")


if __name__ == "__main__":
    run()
