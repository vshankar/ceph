"""
ceph subsystem performance stats
"""

from mgr_module import MgrModule

from .fs.perf_stats import FSPerfStats

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "fs perf stats "
                   "name=mds_rank,type=CephString,req=false",
            "desc": "retrieve ceph fs performance stats",
            "perm": "r"
        },
    ]
    MODULE_OPTIONS = []

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_perf_stats = FSPerfStats(self)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        # only supported command is `fs perf stats` right now
        if prefix.startswith('fs perf stats'):
            return self.fs_perf_stats.get_perf_data(cmd)
        raise NotImplementedError(cmd['prefix'])
