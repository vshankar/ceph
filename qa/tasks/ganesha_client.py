"""
reconfigure a ganesha server
"""
import logging

from teuthology.task import Task

log = logging.getLogger(__name__)

class GaneshaClient(Task):
    def __init__(self, ctx, config):
        super(GaneshaClient, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(GaneshaClient, self).setup()

    def begin(self):
        super(GaneshaClient, self).begin()

        log.info('mounting ganesha client')
        log.info(f'config is {self.config}')

    def end(self):
        super(GaneshaClient, self).end()

task = GaneshaClient
