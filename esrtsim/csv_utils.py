import csv
import logging
import os
import threading
import queue

import pandas as pd

def sort_csv(data_path: str, datastream_name: str, result_queue: queue.Queue) -> None:
    log = logging.getLogger(__name__)
    from_file: str = os.path.join(data_path, f'{datastream_name}.csv')
    to_file: str = os.path.join(data_path, f'_{datastream_name}.csv')
    log.debug(f'Sorting {from_file}')
    pd.read_csv(from_file) \
        .sort_values(by=['time_offset'], axis=0) \
        .to_csv(to_file, sep=',', quoting=csv.QUOTE_NONNUMERIC, index=False)
    log.debug(f'Sorted {from_file} into {to_file}')
    result_queue.put((datastream_name, to_file))

def sort_csvs(datastream_names: list[str], data_path: str = '.') -> dict[str, str]:
    log = logging.getLogger(__name__)
    log.info('Pre-processing CSV files')
    result_queue = queue.Queue(0)
    threads = [ threading.Thread(target=sort_csv, args=(data_path, s, result_queue)) for s in datastream_names ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    result = {}
    try:
        while True:
            (s, f) = result_queue.get_nowait()
            result[s] = f
    except queue.Empty:
        pass
    log.info('Pre-processed CSV files')
    return result