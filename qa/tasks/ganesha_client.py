"""
mount a ganesha client
"""

import json
import logging
from io import StringIO

from teuthology.misc import deep_merge
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)

class GaneshaClient(Task):
    def __init__(self, ctx, config):
        super(GaneshaClient, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(GaneshaClient, self).setup()

    def begin(self):
        super(GaneshaClient, self).begin()
        log.info('mounting ganesha client(s)')

        if self.config is None:
            ids = misc.all_roles_of_type(self.ctx.cluster, 'client')
            client_roles = [f'client.{id_}' for id_ in ids]
            self.config = dict([r, dict()] for r in client_rols)
        elif isinstance(self.config, list):
            client_roles = self.config
            self.config = dict([r, dict()] for r in client_roles)
        elif isinstance(self.config, dict):
            client_roles = filter(lambda x: 'client.' in x, self.config.keys())
        else:
            raise ValueError(f"Invalid config object: {self.config} ({self.config.__class__})")
        log.info(f"config is {self.config}")

        mounts = {}

        clients = list(misc.get_clients(ctx=self.ctx, roles=client_roles))
        test_dir = misc.get_testdir(self.ctx)

        client_config = self.config
        log.info(f'client_config is {client_config}')
        overrides = self.ctx.config.get('overrides', {}).get('ganesha-client', {})
        log.info(f'overrides is {overrides}')

        deep_merge(client_config, overrides)
        log.info(f'client_config is {client_config}')

        cluster_id = client_config['cluster_id']
        pseudo_path = client_config['pseudo_path']

        try:
            first_mon = misc.get_first_mon(self.ctx, None)
            (mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()

            proc = mon0_remote.run(args=['ceph', 'nfs', 'export', 'info', cluster_id, pseudo_path],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            log.debug(f'res: {res} type {type(res)}')
            export_json = json.loads(res)
            log.debug(f'export_json: {export_json} type {type(export_json)}')

            proc = mon0_remote.run(args=['ceph', 'nfs', 'cluster', 'info', cluster_id],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            log.debug(f'res: {res} type {type(res)}')
            cluster_info = json.loads(res)
            log.debug(f'cluster_info: {cluser_info} type {type(cluster_info)}')

            info_output = cluster_info[cluster_id]['backend'][0]
        except Exception as e:
            log.error(f'failed: {e}')

        yield mounts

    def end(self):
        super(GaneshaClient, self).end()

task = GaneshaClient
