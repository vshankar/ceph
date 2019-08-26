import re
import json
import time
import errno
import traceback

from datetime import datetime, timedelta
from threading import Lock, Condition, Thread

QUERY_IDS = "query_ids"
QUERY_LAST_REQUEST = "last_time_stamp"

QUERY_RAW_COUNTERS = "query_raw_counters"

MDS_RANK_ALL = (-1,)

MDS_PERF_QUERY_REGEX_MATCH_ALL = '^(.*)$'
MDS_PERF_QUERY_COUNTERS = [
    'cap_hit_metric',
    'osdc_cache_metric',
    'iops_metric',
    ]

QUERY_EXPIRE_INTERVAL = timedelta(minutes=1)

def extract_mds_ranks_from_spec(mds_rank_spec):
    if not mds_rank_spec:
        return MDS_RANK_ALL
    match = re.match(r'^(\d[,\d]*)$', mds_rank_spec)
    if not match:
        raise ValueError("invalid mds filter spec: {}".format(mds_filter_spec))
    return tuple(int(mds_rank) for mds_rank in match.group(0).split(','))

def extract_mds_ranks_from_report(mds_ranks_str):
    if not mds_ranks_str:
        return []
    return [int(x) for x in mds_ranks_str.split(',')]

class FSPerfStats(object):
    lock = Lock()
    q_cv = Condition(lock)
    r_cv = Condition(lock)

    user_queries = {}

    def __init__(self, module):
        self.module = module
        self.log = module.log
        self.report_processor = Thread(target=self.run)
        self.report_processor.start()

    def run(self):
        try:
            self.log.info("FSPerfStats::report_processor starting...")
            while True:
                with self.lock:
                    self.scrub_expired_queries()
                    self.process_mds_reports()
                    self.r_cv.notify()

                    stats_period = int(self.module.get_ceph_option("mgr_stats_period"))
                    self.q_cv.wait(stats_period)
                self.log.debug("FSPerfStats::tick")
        except Exception as e:
            self.log.fatal("fatal error: {}".format(traceback.format_exc()))

    def cull_mds_entries(self, raw_perf_counters, incoming_metrics):
        # this is pretty straight forward -- find what MDSs are missing from
        # what is tracked vs what we received in incoming report and purge
        # the whole bunch.
        tracked_ranks = raw_perf_counters.keys()
        available_ranks = [int(counter['k'][0][0]) for counter in incoming_metrics]
        for rank in set(tracked_ranks) - set(available_ranks):
            culled = raw_perf_counters.pop(rank)
            self.log.info("culled {0} client entries from rank {1} (laggy: {2})".format(
                len(culled[1]), rank, "yes" if culled[0] else "no"))

    def cull_client_entries(self, raw_perf_counters, incoming_metrics):
        # this is a bitmore involed -- for each rank figure out what clients
        # are missing in incoming report and purge them from our tracked map.
        # but, if this is invoked _after_ cull_mds_entries(), the rank set
        # is same, hence we can just loop based on that assumption.
        ranks = raw_perf_counters.keys()
        for rank in ranks:
            tracked_clients = raw_perf_counters[rank][1].keys()
            available_clients = [counter['k'][1][0] for counter in incoming_metrics]
            for client in set(tracked_clients) - set(available_clients):
                culled = raw_perf_counters[rank][1].pop(client)
                self.log.info("culled {0} from rank {1}".format(culled[1], rank))

    def cull_missing_entries(self, raw_perf_counters, incoming_metrics):
        self.cull_mds_entries(raw_perf_counters, incoming_metrics)
        self.cull_client_entries(raw_perf_counters, incoming_metrics)

    def get_raw_perf_counters(self, mds_ranks, query):
        raw_perf_counters = query.setdefault(QUERY_RAW_COUNTERS, {})

        for query_id in query[QUERY_IDS]:
            result = self.module.get_mds_perf_counters(query_id)
            self.log.debug("get_raw_perf_counters={}".format(result))

            # extract passed in delayed. metrics for delayed ranks are tagged
            # as stale.
            delayed_ranks = extract_mds_ranks_from_report(result['metrics'][0][0])

            # what's received from MDS
            incoming_metrics = result['metrics'][1]

            # cull missing MDSs and clients
            self.cull_missing_entries(raw_perf_counters, incoming_metrics)

            # iterate over metrics list and update our copy (note that we have
            # already culled the differences).
            for counter in incoming_metrics:
                mds_rank = int(counter['k'][0][0])
                client_id = counter['k'][1][0]

                raw_counters = raw_perf_counters.setdefault(mds_rank, [False, {}])
                if mds_rank in delayed_ranks:
                    raw_counters[0] = True
                raw_client_counters = raw_counters[1].setdefault(client_id, [])

                raw_client_counters.clear()
                raw_client_counters.extend(counter['c'])

    def process_mds_reports(self):
        for mds_ranks, query in self.user_queries.items():
            if not query[QUERY_IDS]:
                continue
            self.get_raw_perf_counters(mds_ranks, query)

    def scrub_expired_queries(self):
        expire_time = datetime.now() - QUERY_EXPIRE_INTERVAL
        for mds_ranks in list(self.user_queries.keys()):
            user_query = self.user_queries[mds_ranks]
            self.log.debug("query={}".format(user_query))
            if user_query[QUERY_LAST_REQUEST] < expire_time:
                self.unregister_mds_perf_queries(mds_ranks, user_query[QUERY_IDS])
                del self.user_queries[mds_ranks]

    def prepare_mds_perf_query(self, rank):
        mds_rank_regex = MDS_PERF_QUERY_REGEX_MATCH_ALL
        if not rank == -1:
            mds_rank_regex = '^({})$'.format(rank)
        return {
            'key_descriptor' : [
                {'type' : 'mds_rank', 'regex' : mds_rank_regex},
                {'type' : 'client_id', 'regex' : MDS_PERF_QUERY_REGEX_MATCH_ALL},
                ],
            'performance_counter_descriptors' : MDS_PERF_QUERY_COUNTERS,
            }

    def unregister_mds_perf_queries(self, mds_ranks, query_ids):
        self.log.info("unregister_mds_perf_queries: mds_ranks={0}, query_id={1}".format(
            mds_ranks, query_ids))
        for query_id in query_ids:
            self.module.remove_mds_perf_query(query_id)
        query_ids[:] = []

    def register_mds_perf_query(self, mds_ranks):
        query_ids = []
        try:
            for rank in mds_ranks:
                query = self.prepare_mds_perf_query(rank)
                self.log.info("register_mds_perf_query: {}".format(query))

                query_id = self.module.add_mds_perf_query(query)
                if query_id is None: # query id can be 0
                    raise RuntimeError("failed to add MDS perf query: {}".format(query))
                query_ids.append(query_id)
        except Exception:
            for query_id in query_ids:
                self.module.remove_mds_perf_query(query_id)
            raise
        return query_ids

    def register_query(self, mds_ranks):
        user_query = self.user_queries.get(mds_ranks, None)
        if not user_query:
            user_query = {
                QUERY_IDS : self.register_mds_perf_query(mds_ranks),
                QUERY_LAST_REQUEST : datetime.now(),
                }
            self.user_queries[mds_ranks] = user_query

            self.q_cv.notify()
            self.r_cv.wait(5)
        else:
            user_query[QUERY_LAST_REQUEST] = datetime.now()
        return user_query

    def generate_report(self, user_query):
        raw_perf_counters = user_query.setdefault(QUERY_RAW_COUNTERS, {})
        result = {}
        result["counters"] = MDS_PERF_QUERY_COUNTERS

        for rank, counters in raw_perf_counters.items():
            mds_key = "mds.{}".format(rank)
            result[mds_key] = {}
            result[mds_key]["delayed"] = "yes" if counters[0] else "no"
            result[mds_key]["metrics"] = counters[1]
        return result

    def get_perf_data(self, cmd):
        mds_rank_spec = cmd.get('mds_rank', None)
        mds_ranks = extract_mds_ranks_from_spec(mds_rank_spec)
        self.log.debug("get_perf_data: mds_ranks={}".format(mds_ranks))

        counters = {}
        with self.lock:
            user_query = self.register_query(mds_ranks)
            result = self.generate_report(user_query)
        return 0, json.dumps(result), ""
