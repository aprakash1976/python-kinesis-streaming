from affine.model.streams import StreamState
from affine.model.base import session as db_session
from kinesisbolt import KinesisSpoutUnbundler, KinesisSink
from datetime import datetime, timedelta
from affine.retries import retry_operation


class KinesisSpoutUnbundlerState(KinesisSpoutUnbundler):
    def __init__(self, source_stream, shard_id, processor, *args, **kwargs):
        if ('start_at' not in kwargs):
            kwargs['start_at'] = 'RESUME'
        super(KinesisSpoutUnbundlerState, self).__init__(source_stream, shard_id, *args, **kwargs)
        self._source_stream = source_stream
        self._shard_id = shard_id
        self._processor = processor
        if (kwargs['start_at'] == 'RESUME'):
            stream_state = retry_operation(StreamState.get_or_create, self._processor, self._source_stream, self._shard_id, sleep_time=5)
            db_session.close()
            if (stream_state) and (stream_state.sequence_number) and ((datetime.utcnow() - stream_state.updated_at) < timedelta(hours=1)):
                self._last_seqnum =  stream_state.sequence_number
                self._log.warn('Resume from Stream State processor={0}, shard={1}, seqnum=={2} @ {3}'.format(stream_state.processor, 
                                                                                                             stream_state.shard_id, 
                                                                                                             stream_state.sequence_number,
                                                                                                             stream_state.updated_at))
                return
        self._start_at = kwargs['start_at']
        self._log.warn('Stream State starting from {0}'.format(self._start_at))

    def save_state(self):
        stream_state = retry_operation(StreamState.get_or_create, self._processor, self._source_stream, self._shard_id, sleep_time=5)
        if (stream_state):
            stream_state.sequence_number = self.current_seqnum
            stream_state.updated_at = datetime.utcnow()
            retry_operation(db_session.flush, [], sleep_time=5)
            self._log.warn('stream state saved to DB')
        db_session.close()

    def shutdown(self):
        self._log.warn('Shutdown initiated @ seqnum = {0}'.format(self.current_seqnum))
        self.save_state()
