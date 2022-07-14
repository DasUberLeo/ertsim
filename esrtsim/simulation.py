import csv
import datetime as dt
import logging
from time import sleep
import threading

from elasticsearch import Elasticsearch

from .datastream import DataStream

def bulk_operation(es_client, operations, timestamp):
    log = logging.getLogger(__name__)
    log.debug(f'Bulk transaction: {len(operations) // 2} operations')
    if(timestamp > dt.datetime.now()):
        sleep((timestamp - dt.datetime.now()).seconds)
    return es_client.bulk(operations=operations)

def simulate_data_stream(start_time: dt.datetime, data_stream: DataStream, csv_file: str):
    log = logging.getLogger(__name__)
    log.info(f'Processing "{data_stream.qname}"')

    es_client = data_stream.get_es_client()

    log.debug('Sending CSV contents')
    with open(csv_file, mode='r', newline='') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        last_offset = 0
        timestamp = start_time + dt.timedelta(milliseconds=last_offset)
        operations = []
        for row in csv_reader:
            new_offset = int(row.pop('time_offset'))
            if new_offset == last_offset:
                row['@timestamp'] = timestamp
                operations += [ { 'create' : { '_index' : data_stream.name, '_id' : csv_reader.line_num } }, row ]
            else:
                if len(operations) > 0:
                    bulk_operation(es_client, operations, timestamp)
                last_offset = new_offset
                timestamp = start_time + dt.timedelta(milliseconds=last_offset)
                row['@timestamp'] = timestamp
                operations = [ { 'create' : { '_index' : data_stream.name, '_id' : csv_reader.line_num } }, row ]
        bulk_operation(es_client, operations, timestamp)
    log.debug('Sent CSV contents')

    es_client.close()

    log.info(f'Processed "{data_stream.qname}"')

def run_simulation(data_streams: list[DataStream], csv_files: dict[str, str]):
    log = logging.getLogger(__name__)

    log.info('Simulation Begins')
    start_time = dt.datetime.now()
    threads = [ threading.Thread(target=simulate_data_stream, args=(start_time, s, csv_files[s.qname])) for s in data_streams]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    log.info('Simulation Ends')