import msgpack
import sys
from streaming import msgmodel


(fieldname, value) = sys.argv[1:3]

for ff in sys.argv[3:]:
    with open(ff, "rb") as fin:
        unpacker = msgpack.Unpacker(fin)
        for evt in unpacker:
            eevt = msgmodel.expand(evt)
            if (eevt.get(fieldname) == value):
                print eevt



