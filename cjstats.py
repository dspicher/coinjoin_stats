
from IPython import embed
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from secrets import rpc_password, rpc_user
from collections import Counter
import numpy as np
import pickle
import logging
import concurrent.futures

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


wasabi_address = "bc1qs604c7jv6amk4cxqlnvuxv26hv3e48cds4m0ew"


def handle_block(height):
    rpc_connection = AuthServiceProxy(
        "http://%s:%s@127.0.0.1:8332" % (rpc_user, rpc_password))
    hash = rpc_connection.getblockhash(height)
    block = rpc_connection.getblock(hash, 2)
    cjs = np.zeros((0, 9))
    txids = []
    for tx in block['tx']:
        out_values = Counter([o['value'] for o in tx['vout']])
        m_c = out_values.most_common()
        candidates = filter(lambda m: m[1] > 1, m_c)
        candidates = list(filter(lambda m: m[0] > 0, candidates))
        if len(candidates) == 0:
            continue
        cj = candidates[0]
        addresses = [o['scriptPubKey']['addresses']
                     for o in tx['vout'] if 'addresses' in o['scriptPubKey'].keys()]
        addresses = [item for sublist in addresses for item in sublist]
        is_wasabi = wasabi_address in addresses
        has_op_return = any([out['scriptPubKey']['type'] ==
                             'nulldata' for out in tx['vout']])
        features = [height, len(tx['vin']), len(
            tx['vout']), cj[0], cj[1], max(out_values), min(out_values), has_op_return, is_wasabi]
        cjs = np.vstack((cjs, features))
        txids.append(tx['txid'])
    # logging.info("processed {}, {} cjs".format(height, len(txids)))
    return height, cjs, txids


(all_cjs, all_txids, processed, _) = pickle.load(open('cjs.p', 'rb'))
rpc_connection = AuthServiceProxy(
    "http://%s:%s@127.0.0.1:8332" % (rpc_user, rpc_password))
curr_height = rpc_connection.getblockchaininfo()['blocks']
blocks = list(filter(lambda bl: bl not in processed, range(0, curr_height+1)))

if len(blocks) == 0:
    logging.info(
        "no work to do, collected all CoinJoins up to {}".format(curr_height))
    import sys
    sys.exit()

logging.info("{} blocks remaining, smallest: {}".format(
    len(blocks), min(blocks)))
failures = []
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    future_to_block = {executor.submit(
        handle_block, height): height for height in blocks}
    for future in concurrent.futures.as_completed(future_to_block):
        height = future_to_block[future]
        try:
            height, cjs, txids = future.result()
        except Exception as exc:
            failures.append(height)
            logging.warning('%r generated an exception: %s' % (height, exc))
        else:
            processed.add(height)
            all_cjs = np.vstack((all_cjs, cjs))
            all_txids += txids
            # logging.info("processes height {}: {} cjs".format(
            #     height, len(txids)))
        if height % 1000 == 0:
            logging.info(
                "saving file at height {}, progress: {:.2f}%, {} failures, {} coinjoins".format(
                    height, len(processed)/(len(processed)+len(blocks))*100, len(failures), len(all_txids)))
            pickle.dump((all_cjs, all_txids, processed,
                         failures), open('cjs.p', 'wb'))

logging.info(
    "saving file at height {}, progress: {:.2f}%, {} failures, {} coinjoins".format(
        height, len(processed)/(len(processed)+len(blocks))*100, len(failures), len(all_txids)))
pickle.dump((all_cjs, all_txids, processed,
             failures), open('cjs.p', 'wb'))
