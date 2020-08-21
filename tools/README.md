## Energeia

Energeia is a pure-python emulation of Amazon Kinesis service. It is used for
development and testing on a local workstation. Start the service as:

    python ./energeia.py -D /home/foo/data MyStream:5
    
This will either recover the stream named "MyStream" if found in the data
directory "/home/foo/data" or it will create it. The stream will be sharded
5 ways.

Client apps should monkey-patch the Kinesis boto connection client, e.g.

    import boto
    import kinesis
    boto.connect_kinesis = (lambda: energeia.EnergeiaClient())

Currently, only the following boto API endpoints are available:

    put_record()
    get_records()
    get_shard_iterator()
    describe_stream()
   

### Module prerequisites

The server was developed on python 2.7.2. Other python versions 
have not been tested. 

  + asyncio
  + concurrent.futures

