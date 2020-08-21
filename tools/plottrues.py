from s3msgstreamer import S3MsgStreamer

class TruesByCampaign(S3MsgStreamer):
    def __init__(self, start, end, *args, **kwargs):
        super(self, TruesByCampaign).__init__(start, end, *args, **kwargs)
        
    def process(self, msg):
        if ('impression_id' in msg):
            print msg

    def run(self):
        count = 0
        for msg in s3m.getmsgs():
            count += 1
            if (count > 1000): break
            self.process(msg)
        


