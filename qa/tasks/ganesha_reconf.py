"""
reconfigure a ganesha server
"""

import json
import logging
from io import StringIO

from teuthology.misc import deep_merge
from teuthology.task import Task
from teuthology import misc

log = logging.getLogger(__name__)

class GaneshaReconf(Task):
    def __init__(self, ctx, config):
        super(GaneshaReconf, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(GaneshaReconf, self).setup()

    def begin(self):
        super(GaneshaReconf, self).begin()
        log.info('reconfiguring ganesha server')

        ganesha_config = self.config
        log.info(f'ganesha_config is {ganesha_config}')
        overrides = self.ctx.config.get('overrides', {}).get('ganesha-reconf', {})
        log.info(f'overrides is {overrides}')

        deep_merge(ganesha_config, overrides)
        log.info(f'ganesha_config is {ganesha_config}')

        try:
            first_mon = misc.get_first_mon(self.ctx, None)
            (mon0_remote,) = self.ctx.cluster.only(first_mon).remotes.keys()

            cluster_id = ganesha_config['cluster_id']
            pseudo_path = ganesha_config['pseudo_path']

            proc = mon0_remote.run(args=['ceph', 'nfs', 'export', 'info', cluster_id, pseudo_path],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            log.debug(f'res: {res} type {type(res)}')
            export_json = json.loads(res)
            log.debug(f'export_json: {export_json} type {type(export_json)}')

            is_async = ganesha_config.get('async', False)
            if is_async:
                export_json.setdefault("ceph", {})
                export_json["ceph"]["async"] = True
            is_zerocopy = ganesha_config.get('zerocopy', False)
            if is_async:
                export_json.setdefault("ceph", {})
                export_json["ceph"]["zerocopy"] = True

            if is_async or is_zerocopy:
                mon0_remote.run(args=['ceph', 'nfs', 'export', 'apply', cluster_id, "-i", "-"],
                                      stdin=json.dumps(export_json))

            proc = mon0_remote.run(args=['ceph', 'nfs', 'export', 'info', cluster_id, pseudo_path],
                                   stdout=StringIO(), wait=True)
            res = proc.stdout.getvalue()
            log.debug(f'verify res: {res} type {type(res)}')
            export_json = json.loads(res)
            log.debug(f'verify export_json: {export_json} type {type(export_json)}')
        except Exception as e:
            log.error(f'failed: {e}')

    def end(self):
        super(GaneshaReconf, self).end()

task = GaneshaReconf
