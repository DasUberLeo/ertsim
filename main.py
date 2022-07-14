import logging

import esrtsim as sim

logging.basicConfig(
    level=logging.DEBUG,
    format=sim.LOG_FORMAT
)
log = logging.getLogger(__name__)

log.info('Started')
clusters, data_streams = sim.load_config_file('config.json')
csv_files = sim.sort_csvs([data_stream.qname for data_stream in data_streams])
for cluster in clusters:
    cluster.check_and_delete([s.name for s in data_streams if s.cluster_name == cluster.name])
for data_stream in data_streams:
    data_stream.initialise()
sim.run_simulation(data_streams, csv_files)
log.info('Finished')