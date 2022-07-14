import json
import logging

from elasticsearch import Elasticsearch

from esrtsim.const import JSON_HEADERS

class Cluster:
    def __init__(self, name: str, cluster_config_dict: dict):
        self.name: str = name
        self.es_hosts: str = cluster_config_dict['hosts'] if 'hosts' in cluster_config_dict else None
        self.es_cloud_id: str = cluster_config_dict['cloud_id'] if 'cloud_id' in cluster_config_dict else None
        self.es_username: str = cluster_config_dict['username'] if 'username' in cluster_config_dict else None
        self.es_password: str = cluster_config_dict['password'] if 'password' in cluster_config_dict else None
    
    def get_es_client(self) -> Elasticsearch:
        return Elasticsearch(
            hosts=self.es_hosts,
            cloud_id=self.es_cloud_id,
            basic_auth=(self.es_username, self.es_password)
        )
    
    def ___check_and_delete(self, es_client: Elasticsearch, object_type: str, names: list[str], suffixes: list[str]):
        log = logging.getLogger(__name__)
        log.debug(f'Checking for existing {object_type}s')
        response = es_client.perform_request(method='GET', path=f'/_{object_type}')
        if suffixes is not None and len(suffixes) > 0:
            names = [f'{n}-{s}' if len(s) > 0 else f'{n}' for s in suffixes for n in names]
        for object in response[f'{object_type}s']:
            object_name = f'{object["name"]}'
            if object_name in names:
                logging.debug(f'Deleting {object_name}')
                es_client.perform_request(method='DELETE', path=f'/_{object_type}/{object_name}')
                logging.debug(f'Deleted {object_name}')
        log.debug(f'Checked for existing {object_type}s')
    
    def check_and_delete(self, names: list[str]):
        log = logging.getLogger(__name__)
        log.info('Checking for existing objects')
        log.debug(f'Names {names}')
        es_client = self.get_es_client()
        self.___check_and_delete(es_client, 'data_stream', names, [])
        self.___check_and_delete(es_client, 'index_template', names, [])
        self.___check_and_delete(es_client, 'component_template', names, ['mappings', 'settings'])
        es_client.close()
        log.info('Checked for existing objects')

class DataStream:
    def __init__(self, cluster: Cluster, name: str, data_stream_config_dict: dict):
        mappings_dict = data_stream_config_dict['mappings']
        self.___cluster: Cluster = cluster
        self.___name: str = name
        self.___qname: str = f'{cluster.name}.{name}'
        self.___mappings: dict = mappings_dict
        self.___mappings['properties']['@timestamp'] = { 'type': 'date', 'format': 'date_optional_time||epoch_millis' }
        self.___settings: dict = None
    
    @property
    def name(self) -> str:
        return self.___name
    
    @property
    def cluster_name(self) -> str:
        return self.___cluster.name
    
    @property
    def qname(self) -> str:
        return self.___qname

    @property
    def mappings(self) -> dict:
        return self.___mappings
    
    def get_es_client(self) -> Elasticsearch:
        return self.___cluster.get_es_client()

    def initialise(self) -> None:
        log = logging.getLogger(__name__)

        log.info(f'Initialising {self.___name}s')
        es_client = self.___cluster.get_es_client()

        log.debug(f'Creating component_template {self.___name}-mappings')
        composed_of = [ f'{self.___name}-mappings' ]

        log.debug(json.dumps({ 'template': { 'mappings': self.___mappings } }))
        es_client.perform_request(
            method='PUT',
            path=f'/_component_template/{self.___name}-mappings', 
            headers=JSON_HEADERS,
            body=json.dumps({ 'template': { 'mappings': self.___mappings } })
        )
        log.debug(f'Created component_template {self.___name}-mappings')

        if(self.___settings is not None):
            log.debug(f'Creating component_template {self.___name}-settings')
            composed_of += [ f'{self.___name}-settings' ]
            es_client.perform_request(
                method='PUT',
                path=f'/_component_template/{self.___name}-settings', 
                headers=JSON_HEADERS,
                body=json.dumps({ 'template': { 'settings': self.___settings } })
            )
            log.debug(f'Created component_template {self.___name}-settings')

        log.debug(f'Creating index_template {self.___name}')
        es_client.perform_request(
            method='PUT',
            path=f'/_index_template/{self.___name}', 
            headers=JSON_HEADERS,
            body=json.dumps({
                'index_patterns': [ f'{self.___name}*' ],
                'data_stream': { },
                'composed_of': composed_of,
                'priority': 500
            })
        )
        log.debug(f'Created index_template {self.___name}')

        es_client.close()
        log.info('Initialised {self.___name}s')