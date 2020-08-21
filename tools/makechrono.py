import sys
import datetime
import collections
import os
from s3zfile import S3ZipFile
import boto
import gzip
import streaming.msgmodel as msgmodel
import msgpack


#
#   Reorders event log files off S3 into correct chronological order
#   and saves on local filesystem. Also, bundles into event by common
#   request_id
#


def mapresponse(text_resp):
    if (text_resp == "True"): return True
    return [None, False][(text_resp == "False")]


def decompress(stream):
    decompressor = zlib.decompressobj(16+zlib.MAX_WBITS)
    for chunk in stream:
        rv = decompressor.decompress(chunk)
        if (rv):
            yield rv
    

def s3_get_next_line(s3key_stream):
    rem = ""
    for buf in decompressor(s3key_stream):
        buf = rem + buf
        while (len(buf)>0):
            idx = buf.find('\n')
            if (idx < 0):
                rem = buf[:]
                break
            line = buf[:idx+1]
            buf  = buf[idx+1:]
            yield line


def tsv2evt(tsv):
    d = dict(zip(msgmodel.event_field_names, tsv))
    e = { k:v for (k,v) in d.iteritems() if (len(v) > 0) }
    return e

def split_by_time(s3key):
    min_ts = None
    last_ts = None
    fnames = []
    ofile = None
    s3zf = S3ZipFile(s3key)
    for line in s3zf.next_line():
        if (len(line) == 0): break
        v = line.strip("\n").split("\t")
        ts = datetime.datetime.strptime(v[0], "%Y-%m-%d %H:%M:%S")
        if (not last_ts): last_ts = ts
        if (not min_ts): min_ts = ts
        delta = ts - last_ts
        min_ts = min(min_ts, ts)
        if (delta.total_seconds() < -20):
            print ts, delta.total_seconds()
            ofile.close()
            ofile = None
        
        if (ofile is None):
            fnames.append("oo%03d" % len(fnames))
            ofile = open(fnames[-1], "w")
        last_ts = ts
        ofile.write(line)
    ofile.close()
    return (min_ts, fnames)


def join_parts(min_ts, outputname, fparts):
    iif = { fof: open(fof, "r") for fof in fparts }
    xclock = min_ts
    yend = xclock + datetime.timedelta(seconds=100)
    pending = collections.defaultdict(list)
    unf = gzip.open(outputname, "wb")
    mm = open(outputname.replace("U","Y"), "wb")
    xcnt = 0

    while (len(iif) > 0):
        bund = collections.defaultdict(list)

        while (len(pending[xclock])>0):
            (rqid,xx) = pending[xclock].pop()
            unf.write(xx)
            bund[rqid].append(tsv2evt(xx.strip("\n").split("\t")))
        del pending[xclock]
        
        
        for fidx in iif.keys():
            ifz = iif[fidx]
            if (not ifz): continue
            while (True):
                xx = ifz.readline()
                if (len(xx) == 0): 
                    ifz.close()
                    del iif[fidx]
                    print xclock, xcnt, len(iif), len(pending)
                    break
                xcnt += 1
                v = xx.strip("\n").split("\t")
                ts = datetime.datetime.strptime(v[0], "%Y-%m-%d %H:%M:%S")
                if (ts > xclock):
                    pending[ts].append((v[2],xx))
                    break
                unf.write(xx)
                bund[v[2]].append(tsv2evt(v))

        for reqid in bund.keys():
            liv = []
            e0 = None
            while (len(bund[reqid])):
                evt = bund[reqid].pop()
                if (evt['event_type'] == 'query'):
                    if (not e0):
                        e0 = {k:v for (k,v) in evt.items() if (k in msgmodel.query_fixed_fields)}
                    liv.append((evt['line_item_id'], mapresponse(evt['response']), evt.get('reason_code'), evt.get('reason_details')))
                else:
                    mm.write(msgpack.packb(msgmodel.compact(evt), use_bin_type=True))
            if (e0) and (liv):
                e0['line_item_responses'] = liv
                mm.write(msgpack.packb(msgmodel.compact(e0), use_bin_type=True))
                
        del bund
        xclock += datetime.timedelta(seconds=1)


    while (len(pending[xclock])>0):
        (rqid,xx) = pending[xclock].pop()
        unf.write(xx)
    unf.close()

    for fof in fparts:
        os.remove(fof)
    


def timesnap_series(tstart, tend, tdelta):
    tnow = datetime.datetime.now()
    xmin = (tstart.minute/15)*15
    dt = tstart.replace(minute=xmin)
    while (dt < tend):
        yield dt
        dt += tdelta


if __name__ == "__main__":
    usage_string = "usage: makechrono.py <instance_id> <YYYY-mm-DD_HH_MM> <YYYY-mm-DD_HH_MM>\n"
    #
    #  cmd-line args
    #
    cmd_line_setters = {}
    opts = collections.defaultdict(bool)
    if (len(sys.argv) < 4):
        sys.stderr.write(usage_string)
        sys.exit(1)
    (instance_id, zstartt, zendt) = sys.argv[-3:]
    try:
        startt = datetime.datetime.strptime(zstartt, "%Y-%m-%d_%H_%M")
        endt = datetime.datetime.strptime(zendt, "%Y-%m-%d_%H_%M")
    except:
        sys.stderr.write(usage_string)
        sys.exit(1)

    if (endt < startt):
        sys.stderr.write(usage_string)
        sys.exit(1)
        
    print instance_id, startt, endt
    
    

    min_ts = startt+datetime.timedelta(minutes=-15)
    s3c = boto.connect_s3()
    s3bucket = "affine-hadoop-input"
    buck = s3c.get_bucket(s3bucket)
    tdelt = datetime.timedelta(minutes=15)
    s3keybase = os.path.join("ads_servers", instance_id, "events")
    for dt in timesnap_series(startt, endt, tdelt):
        fname = "events.log." + dt.strftime("%Y-%m-%d_%H_%M") + ".gz"
        s3keyname = os.path.join(s3keybase, fname)
        print s3keyname
        s3key = buck.get_key(s3keyname)
        if (not s3key):
            print "cannot find S3 key %s in %s bucket" % (s3keyname, s3bucket)
            continue
        (min_ts, fnames) = split_by_time(s3key)
#        fnames = [ "oo%03d" % iid for iid in xrange(8) ]
        outputn = 'U_' + os.path.basename(s3keyname)
        join_parts(min_ts, outputn, fnames)


