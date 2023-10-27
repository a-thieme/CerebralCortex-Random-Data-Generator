import datetime

import generator

import socket
import json

import time

import argparse

class Sender:
    def __init__(self, port):
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.conn.connect(('localhost', port))

    def send(self, payload):
        self.conn.send(payload.encode('utf-8'))

    def close(self):
        self.conn.close()

def get_sender():
    # todo: make this port configurable at runtime
    port = 15000
    print('Sender initialize')
    sender = Sender(port)
    return sender

def send_stream(data_to_send, sender_obj):

    serialize_data = json.dumps(data_to_send)

    ack = ''
    while not ack:
        sender_obj.conn.send(serialize_data.encode())
        ack = sender_obj.conn.recv(1024).decode()

    print("From server: ", ack)
    sender_obj.conn.close()

def format_time(dt:datetime.datetime):
    return int(time.mktime(dt.timetuple()))

def main(rows_per_batch = 5, batches = 5, batch_wait = 0, stream_wait = 0):
    """
     Wrapper code for data generation and transport to producer via socket
     @var batches int number of times we will generate the data and send it
    """
    parser = argparse.ArgumentParser(
        prog='Data Generator',
        description='Random Data Generator for mGuard System',
    )
    parser.add_argument('-b', '--batches', action='store')
    parser.add_argument('-r', '--rows-per-batch', action='store')
    parser.add_argument('-bw', '--wait-between-batch', action='store')
    parser.add_argument('-sw', '--wait-between-stream', action='store')
    a = parser.parse_args()

    if a.batches is not None:
        batches = int(a.batches)

    if a.rows_per_batch is not None:
        rows_per_batch = int(a.rows_per_batch)

    if a.wait_between_batch is not None:
        batch_wait = int(a.wait_between_batch)

    if a.wait_between_stream is not None:
        stream_wait = int(a.wait_between_stream)

    print(f'rpb {rows_per_batch}')
    print(f'batch {batches}')
    print(f'bwait {batch_wait}')
    print(f'swait {stream_wait}')

    working_start = datetime.datetime.utcnow().replace(microsecond=0)
    delta_time = datetime.timedelta(seconds=rows_per_batch)
    one_second = datetime.timedelta(seconds=1)

    for batch_number in range(batches):
        working_end = working_start + delta_time

        print("Fetching data for formatted_start_time {} and formatted_end_time {}".format(format_time(working_start), format_time(working_end)))
        cc_obj, streams = generator.get_cc(working_start, working_end)

        for stream in streams:
            stream_name = streams[stream]

            metadata =  cc_obj.get_stream_metadata_by_name(stream_name).to_json()
            payload = cc_obj.get_stream(stream_name).toPandas()

            data_to_send = {'header': metadata, 'payload': payload.to_csv()}

            # uncomment for debugging
            data1 = payload
            data1.to_csv(str(batch_number) + '_' + stream_name, index=False)

            print("Sending data for stream name {}".format(stream_name))
            # print("Metadata of the stream {} = {}".format(stream_name, metadata))
            
            # sender_obj = get_sender()
            # send_stream(data_to_send, sender_obj)
            
            time.sleep(stream_wait)

        print("Sending data for batch: {}, completed".format(batch_number))

        print("Sleeping for {} second sending batch data".format(batch_wait))
        time.sleep(batch_wait)
        working_start = working_end + one_second

    print ("Sending data for all the batch completed")

if __name__ == '__main__':

    main()