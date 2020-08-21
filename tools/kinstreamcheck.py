import boto
import time
from streaming.kinesisbolt import KinesisSpout



record_count = 0
def endofrecord_cb(*args, **kwargs):
    global record_count
    record_count += 1


def kinstream_active(stream, timeout=20):
    global record_count
    record_count = 0
    skconn = boto.connect_kinesis()
    try:
        streaminfo = skconn.describe_stream(stream)['StreamDescription']
    except:
        return False
    skconn.close()
    shards = sorted(streaminfo['Shards'], key=(lambda x: x['ShardId']))
    ks = KinesisSpout(stream, shards[0]['ShardId'], start_at="LATEST", eob_cb=endofrecord_cb)
    t0 = time.time()
    while ((record_count < 2) and ((time.time() - t0) < timeout)):
        rec = ks.get_next_event(nowait=True)
        if (len(rec)):
            t0 = time.time()
        else:
            time.sleep(1.0)
    return (record_count>1)



if __name__ == "__main__":
    import logging
    logfmt = logging.Formatter("%(message)s")
    logger = logging.getLogger("")
    logger.setLevel(logging.WARN)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logfmt)
    logger.addHandler(ch)
    ok = kinstream_active("AdsQueryCoalesced.dev")
    print ok
    ok = kinstream_active("AdsQueryCoalesced.2xa", timeout=5)
    print ok


    
