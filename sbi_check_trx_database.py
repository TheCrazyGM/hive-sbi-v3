from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
import dataset
import json
from time import sleep
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB


if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    with open(config_file) as json_data_file:
        config_data = json.load(json_data_file)
    print(config_data)
    accounts = config_data["accounts"]
    databaseConnector = config_data["databaseConnector"]
    databaseConnector2 = config_data["databaseConnector2"]
    other_accounts = config_data["other_accounts"]
    mgnt_shares = config_data["mgnt_shares"]
    hive_blockchain = config_data["hive_blockchain"]
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)

    # Update current node list from @fullnodeupdate
    # nodes = NodeList()
    # nodes.update_nodes()
    # stm = Steem(node=nodes.get_nodes())
    data = trxStorage.get_all_data()
    status = {}
    share_type = {}
    n_records = 0
    shares = 0
    for op in data:
        if op["status"] in status:
            status[op["status"]] += 1
        else:
            status[op["status"]] = 1
        if op["share_type"] in share_type:
            share_type[op["share_type"]] += 1
        else:
            share_type[op["share_type"]] = 1
        shares += op["shares"]
        n_records += 1
    print("the trx database has %d records" % (n_records))
    print("Number of shares:")
    print("shares: %d" % shares)
    print("status:")
    for s, value in status.items():
        print("%d status entries with %s" % (value, s))
    print("share_types:")
    for s, value_ in share_type.items():
        print("%d share_type entries with %s" % (value_, s))

    accountTrx = {account: AccountTrx(db, account) for account in accounts}
    sbi_ops = accountTrx["steembasicincome"].get_all()
    last_index = - 1
    for op in trxStorage.get_all_data_sorted():
        if op["source"] != "steembasicincome":
            continue
        if op["index"] - last_index:
            start_index = last_index
            
        