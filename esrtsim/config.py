import json
import logging
from typing import Tuple

from .datastream import Cluster, DataStream

def load_config(config_dict: dict):
    log = logging.getLogger(__name__)
    log.debug('Loading Config Dictionary')
    clusters: list[Cluster] = []
    data_streams: list[DataStream] = []
    for cluster_name in config_dict:
        cluster = Cluster(cluster_name, config_dict[cluster_name])
        clusters.append(cluster)
        ds_dict = config_dict[cluster_name]['data_streams']
        for data_stream_name in ds_dict:
            data_streams.append(DataStream(cluster, data_stream_name, ds_dict[data_stream_name]))
    log.debug('Loaded Config Dictionary')
    return (clusters, data_streams)
    
def load_config_file(config_file: str):
    log = logging.getLogger(__name__)
    log.info(f'Loading {config_file}')
    with open(config_file) as config_file_io:
        config_dict : dict=json.load(config_file_io)
    conf = load_config(config_dict)
    log.info(f'Loaded {config_file}')
    return conf