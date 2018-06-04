#!/usr/bin/env python

# Copyright (c)2015 Rackspace US, Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import datetime
import getpass
import signal
import sys
import time

from elasticsearch import Elasticsearch
from urllib3.util import parse_url

CLUSTER_TEMPLATE = {}
CLUSTER_TEMPLATE['general'] = """{cluster_name:33} {status:6}"""
CLUSTER_TEMPLATE['shards'] = """{active_shards:>6} {active_primary_shards:>4} {relocating_shards:>4} {initializing_shards:>4} {unassigned_shards:>8}"""
CLUSTER_TEMPLATE['tasks'] = """{number_of_pending_tasks:>13}"""
CLUSTER_TEMPLATE['time'] = """{timestamp:8}"""
CLUSTER_HEADINGS = {}
CLUSTER_HEADINGS["cluster_name"] = "cluster"
CLUSTER_HEADINGS["status"] = "status"
CLUSTER_HEADINGS["active_shards"] = "shards"
CLUSTER_HEADINGS["active_primary_shards"] = "pri"
CLUSTER_HEADINGS["relocating_shards"] = "relo"
CLUSTER_HEADINGS["initializing_shards"] = "init"
CLUSTER_HEADINGS["unassigned_shards"] = "unassign"
CLUSTER_HEADINGS["number_of_pending_tasks"] = "pending tasks"
CLUSTER_HEADINGS["timestamp"] = "time"
CLUSTER_CATEGORIES = ['general', 'shards', 'tasks', 'time']

NODES_TEMPLATE = {}
NODES_TEMPLATE['general'] = """{name:24} {role:<6}"""
NODES_TEMPLATE['os'] = """{load_avg:>18} {used_mem:>4}"""
NODES_TEMPLATE['jvm'] = """{used_heap:>4}  {old_gc_sz:8} {old_gc:8} {young_gc:8}"""
NODES_TEMPLATE['threads'] = """{threads:<8}"""
NODES_TEMPLATE['fielddata'] = """{fielddata:^7}"""
NODES_TEMPLATE['connections'] = """{http_conn:>6} {transport_conn:>6}"""
NODES_TEMPLATE['data_nodes'] = """{merge_time:>8} {store_throttle:>8} {fs:>16}  {docs}"""
NODES_FAILED_TEMPLATE = """{name:24} {role:<6}       (No data received, node may have left cluster)"""
NODE_HEADINGS = {}
NODE_HEADINGS["name"] = "nodes"
NODE_HEADINGS["role"] = "role"
NODE_HEADINGS["load_avg"] = "load"
NODE_HEADINGS["used_mem"] = "mem"
NODE_HEADINGS["used_heap"] = "heap"
NODE_HEADINGS["old_gc_sz"] = "old sz"
NODE_HEADINGS["old_gc"] = "old gc"
NODE_HEADINGS["young_gc"] = "young gc"
NODE_HEADINGS["fielddata"] = "fde|fdt"
NODE_HEADINGS["http_conn"] = "hconn"
NODE_HEADINGS["transport_conn"] = "tconn"
NODE_HEADINGS["merge_time"] = "merges"
NODE_HEADINGS["store_throttle"] = "idx st"
NODE_HEADINGS["docs"] = "docs"
NODE_HEADINGS["fs"] = "disk usage"
DEFAULT_THREAD_POOLS = ["index", "search", "bulk", "get"]
CATEGORIES = ['general', 'os', 'jvm', 'threads', 'fielddata', 'connections', 'data_nodes']

class ESArgParser(argparse.ArgumentParser):
    """ArgumentParser which prints help by default on any arg parsing error"""
    def error(self, message):
        self.print_help()
        sys.exit(2)

class ESColors:
    """ANSI escape codes for color output"""
    END = '\033[00m'
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[0;33m'
    GRAY = '\033[1;30m'
    WHITE = '\033[1;37m'

class Elasticstat:
    """Elasticstat"""

    STATUS_COLOR = {'red': ESColors.RED, 'green': ESColors.GREEN, 'yellow': ESColors.YELLOW}

    def __init__(self, args):
        self.sleep_interval = args.delay_interval
        self.node_counters = {}
        self.node_counters['gc'] = {}
        self.node_counters['fd'] = {}
        self.node_counters['hconn'] = {}
        self.nodes_list = [] # used for detecting new nodes
        self.nodes_by_role = {} # main list of nodes, organized by role
        self.node_names = {} # node names, organized by id
        self.new_nodes = [] # used to track new nodes that join the cluster
        self.active_master = ""
        self.no_color = args.no_color
        self.threadpools = self._parse_threadpools(args.threadpools)
        self.categories = self._parse_categories(args.categories)
        self.cluster_categories = CLUSTER_CATEGORIES
        if args.no_pending_tasks:
            # Elasticsearch pre v.1.5 does not include number of pending tasks in cluster health
            self.cluster_categories.remove('tasks')

        # Create Elasticsearch client
        self.es_client = Elasticsearch(self._parse_connection_properties(args.hostlist, args.port, args.username,
                                                                         args.password, args.use_ssl))

    def _parse_connection_properties(self, host, port, username, password, use_ssl):
        hosts_list = []

        if isinstance(host, str):
            # Force to a list, split on ',' if multiple
            host = host.split(',')

        for entity in host:
            # Loop over the hosts and parse connection properties
            host_properties = {}

            parsed_uri = parse_url(entity)
            host_properties['host'] = parsed_uri.host
            if parsed_uri.port is not None:
                host_properties['port'] = parsed_uri.port
            else:
                host_properties['port'] = port

            if parsed_uri.scheme == 'https' or use_ssl is True:
                host_properties['use_ssl'] = True

            if parsed_uri.auth is not None:
                host_properties['http_auth'] = parsed_uri.auth
            elif username is not None:
                if password is None or password == 'PROMPT':
                    password = getpass.getpass()
                host_properties['http_auth'] = (username, password)

            hosts_list.append(host_properties)
        return hosts_list

    def _parse_categories(self, categories):
        if isinstance(categories, list):
            if categories[0] == 'all':
                return CATEGORIES
            if ',' in categories[0]:
                categories = categories[0].split(',')
        else:
            if categories == 'all':
                return CATEGORIES
        for category in categories:
            if category not in CATEGORIES:
                msg = "{0} is not valid, please choose categories from {1}".format(category, ', '.join(CATEGORIES[1:]))
                raise argparse.ArgumentTypeError(msg)
        return ['general'] + categories

    def _parse_threadpools(self, threadpools):
        if isinstance(threadpools, list) and ',' in threadpools[0]:
            threadpools = threadpools[0].split(',')
        return threadpools

    def colorize(self, msg, color):
        if self.no_color is True:
            return(msg)
        else:
            return(color + msg + ESColors.END)

    def thetime(self):
        return datetime.datetime.now().strftime("%H:%M:%S")

    def size_human(self, size):
        for unit in ['B','KB','MB','GB','TB','PB','EB','ZB']:
            if abs(size) < 1024.0:
                return "{:6.2f} {}".format(size, unit)
            size /= 1024.0
        return "{:6.2f} {}".format(size, 'YB')

    def get_disk_usage(self, node_fs_stats):
        # Calculate used disk space
        if node_fs_stats["total"] == {}:
            # Not a data node
            return "-"

        total_in_bytes = node_fs_stats["total"]["total_in_bytes"]
        used_in_bytes = total_in_bytes - node_fs_stats["total"]["available_in_bytes"]

        used_percent = int((float(used_in_bytes) / float(total_in_bytes)) * 100)
        used_human = self.size_human(used_in_bytes)

        return "{}|{}%".format(used_human, used_percent)

    def get_role(self,node_id, node_stats):
        try:
            # Section to handle ES 5
            role = node_stats['nodes'][node_id]['roles']
            if 'data' in role:
                return "DATA"
            if 'master' in role:
                return "MST"
            if 'ingest' in role:      
                return "ING"
            else:
                return "UNK"
        except KeyError:
            # Section to handle ES < 2.x
            ismaster = 'true'
            isdata = 'true'
            role = node_stats['nodes'][node_id]['attributes']
            if 'data' in role:
                isdata = role['data']
            if 'master' in role:
                ismaster = role['master']
            if ismaster == 'true' and isdata == 'true':
                return "ALL"
            elif ismaster == 'true' and isdata == 'false':
                return "MST"
            elif ismaster == 'false' and isdata == 'true':
                return "DATA"
            elif ismaster == 'false' and isdata == 'false':
                return "RTR"
            else:
                return "UNK"
        else: 
            # Section to handle ES 6.x 
            role = node_stats['nodes'][node_id]['nodeRole']
            if 'data' in role:
                return "DATA"
            if 'master' in role:
                return "MST"
            if 'ingest' in role:      
                return "ING"
            else:
                return "UNK"

    def get_gc_stats(self, node_id, node_gc_stats):
        # check if this is a new node
        if node_id not in self.node_counters['gc']:
            # new so init counters and return no data
            self.node_counters['gc'][node_id] = {'old': 0, 'young': 0}
            self.node_counters['gc'][node_id]['old'] = node_gc_stats['old']['collection_count']
            self.node_counters['gc'][node_id]['young'] = node_gc_stats['young']['collection_count']
            return("-|-", "-|-")
        else:
            # existing node, so calculate the new deltas, update counters, and return results
            old_gc_count = node_gc_stats['old']['collection_count']
            young_gc_count = node_gc_stats['young']['collection_count']
            old_gc_delta = old_gc_count - self.node_counters['gc'][node_id]['old']
            young_gc_delta = young_gc_count - self.node_counters['gc'][node_id]['young']
            self.node_counters['gc'][node_id]['old'] = old_gc_count
            self.node_counters['gc'][node_id]['young'] = young_gc_count
            old_gc_results = "{0}|{0}ms".format(old_gc_delta, node_gc_stats['old']['collection_time_in_millis'])
            young_gc_results = "{0}|{0}ms".format(young_gc_delta, node_gc_stats['young']['collection_time_in_millis'])
            return(old_gc_results, young_gc_results)

    def get_fd_stats(self, node_id, current_evictions, current_tripped):
        # check if this is a new node
        if node_id not in self.node_counters['fd']:
            # new so init counters and return no data
            self.node_counters['fd'][node_id] = {'fde': 0, 'fdt': 0}
            self.node_counters['fd'][node_id]['fde'] = current_evictions
            self.node_counters['fd'][node_id]['fdt'] = current_tripped
            return("-|-")
        else:
            # existing node, so calc new deltas, update counters, and return results
            fde_delta = current_evictions - self.node_counters['fd'][node_id]['fde']
            self.node_counters['fd'][node_id]['fde'] = current_evictions
            fdt_delta = current_tripped - self.node_counters['fd'][node_id]['fdt']
            self.node_counters['fd'][node_id]['fdt'] = current_tripped
            return("{0}|{1}".format(fde_delta, fdt_delta))

    def get_http_conns(self, node_id, http_conns):
        # check if this is a new node
        if node_id not in self.node_counters['hconn']:
            self.node_counters['hconn'][node_id] = http_conns['total_opened']
            return ("{0}|-".format(http_conns['current_open']))
        else:
            open_delta = http_conns['total_opened'] - self.node_counters['hconn'][node_id]
            self.node_counters['hconn'][node_id] = http_conns['total_opened']
            return("{0}|{1}".format(http_conns['current_open'], open_delta))

    def process_node_general(self, role, node_id, node):
        if node_id in self.new_nodes:
            # Flag that this is a node that joined the cluster this round
            node_name = node['name'] + "+"
        else:
            node_name = node['name']
        if self.active_master == node_id:
            # Flag active master in role column
            node_role = role + "*"
        else:
            node_role = role
        return(NODES_TEMPLATE['general'].format(name=node_name, role=node_role))

    def process_node_os(self, role, node_id, node):
        if 'cpu' in node['os'] and 'load_average' in node['os']['cpu']:
            # Elasticsearch 5.x+ move load average to cpu key
            node_load_avgs = []
            for load_avg in node['os']['cpu']['load_average'].values():
                node_load_avgs.append(load_avg)
            node_load_avg = "/".join("{0:.2f}".format(x) for x in node_load_avgs)
        else:
            # Pre Elasticsearch 5.x
            node_load_avg = node['os'].get('load_average')
            if isinstance(node_load_avg, list):
                node_load_avg="/".join(str(x) for x in node_load_avg)
            elif isinstance(node_load_avg, float):
                # Elasticsearch 2.0-2.3 only return 1 load average, not the standard 5/10/15 min avgs
                node_load_avg = "{0:.2f}".format(node_load_avg)
            else:
                node_load_avg = 'N/A'

        if 'mem' in node['os']:
            node_used_mem = "{0}%".format(node['os']['mem']['used_percent'])
        else:
            node_used_mem = "N/A"
        return(NODES_TEMPLATE['os'].format(load_avg=node_load_avg, used_mem=node_used_mem))

    def process_node_jvm(self, role, node_id, node):
        processed_node_jvm = {}
        processed_node_jvm['used_heap'] = "{0}%".format(node['jvm']['mem']['heap_used_percent'])
        processed_node_jvm ['old_gc_sz'] = node['jvm']['mem']['pools']['old']['used']
        node_gc_stats = node['jvm']['gc']['collectors']
        processed_node_jvm['old_gc'], processed_node_jvm['young_gc'] = self.get_gc_stats(node_id, node_gc_stats)
        return(NODES_TEMPLATE['jvm'].format(**processed_node_jvm))

    def process_node_threads(self, role, node_id, node):
        thread_segments = []
        for pool in self.threadpools:
            if pool in node['thread_pool']:
                threads ="{0}|{1}|{2}".format(node['thread_pool'][pool]['active'],
                                              node['thread_pool'][pool]['queue'],
                                              node['thread_pool'][pool]['rejected'])
                thread_segments.append(NODES_TEMPLATE['threads'].format(threads=threads))
            else:
                thread_segments.append(NODES_TEMPLATE['threads'].format(threads='-|-|-'))
        return(" ".join(thread_segments))

    def process_node_fielddata(self, role, node_id, node):
        fielddata = self.get_fd_stats(node_id,
                                      node['indices']['fielddata']['evictions'],
                                      node['breakers']['fielddata']['tripped'])
        return(NODES_TEMPLATE['fielddata'].format(fielddata=fielddata))

    def process_node_connections(self, role, node_id, node):
        processed_node_conns = {}
        if node.get('http') == None:
            node['http'] = {u'total_opened': 0, u'current_open': 0}
        processed_node_conns['http_conn'] = self.get_http_conns(node_id, node['http'])
        processed_node_conns['transport_conn'] = node['transport']['server_open']
        return(NODES_TEMPLATE['connections'].format(**processed_node_conns))

    def process_node_data_nodes(self, role, node_id, node):
        processed_node_dn = {}
        # Data node specific metrics
        if role in ['DATA', 'ALL']:
            processed_node_dn['merge_time'] = node['indices']['merges']['total_time']
            processed_node_dn['store_throttle'] = node['indices']['store']['throttle_time']
            doc_count = node['indices']['docs']['count']
            deleted_count = node['indices']['docs']['deleted']
            if deleted_count > 0:
                processed_node_dn['docs'] = "{0}|{1}".format(doc_count, deleted_count)
            else:
                processed_node_dn['docs'] = str(doc_count)
            processed_node_dn['fs'] = self.get_disk_usage(node['fs'])
        else:
            processed_node_dn['merge_time'] = "-"
            processed_node_dn['store_throttle'] = "-"
            processed_node_dn['docs'] = "-"
            processed_node_dn['fs'] = "-"
        return(NODES_TEMPLATE['data_nodes'].format(**processed_node_dn))

    def process_node(self, role, node_id, node):
        node_segments = []
        for category in self.categories:
            category_func = getattr(self, 'process_node_' + category)
            node_segments.append(category_func(role, node_id, node))
        return("   ".join(node_segments))

    def process_role(self, role, nodes_stats):
        procs = []
        for node_id in self.nodes_by_role[role]:
            if node_id not in nodes_stats['nodes']:
                # did not get any data on this node, likely it left the cluster
                # ...however it may have re-joined the cluster under a new node_id (such as a node restart)
                failed_node_name = self.node_names[node_id]
                new_nodes_by_name = {nodes_stats['nodes'][id]['name']: id for id in self.new_nodes}
                if failed_node_name in new_nodes_by_name:
                    # ...found it!  Remove the old node_id, we've already added the new node_id at this point
                    new_node_id = new_nodes_by_name[failed_node_name]
                    self.nodes_list.remove(node_id)
                    self.node_names.pop(node_id)
                    self.nodes_by_role[role].remove(node_id)
                else:
                    failed_node = {}
                    failed_node['name'] = failed_node_name + '-'
                    failed_node['role'] = "({0})".format(role) # Role it had when we last saw this node in the cluster
                    print self.colorize(NODES_FAILED_TEMPLATE.format(**failed_node), ESColors.GRAY)
                continue
            # make sure node's role hasn't changed
            current_role = self.get_role(node_id, nodes_stats)
            if current_role != role:
                # Role changed, update lists so output will be correct on next iteration
                self.nodes_by_role.setdefault(current_role, []).append(node_id) # add to new role
                self.nodes_by_role[role].remove(node_id) # remove from current role
            row = self.process_node(current_role, node_id, nodes_stats['nodes'][node_id])
            if node_id in self.new_nodes:
                print self.colorize(row, ESColors.WHITE)
            else:
                print row

    def get_threads_headings(self):
        thread_segments = []
        for pool in self.threadpools:
            thread_segments.append(NODES_TEMPLATE['threads'].format(threads=pool))
        return(" ".join(thread_segments))

    def format_headings(self):
        """Format both cluster and node headings once and then store for later output"""
        cluster_heading_segments = []
        node_heading_segments = []

        # cluster headings
        for category in self.cluster_categories:
            cluster_heading_segments.append(CLUSTER_TEMPLATE[category].format(**CLUSTER_HEADINGS))
        self.cluster_headings = "   ".join(cluster_heading_segments)

        # node headings
        for category in self.categories:
            if category == 'threads':
                node_heading_segments.append(self.get_threads_headings())
            else:
                node_heading_segments.append(NODES_TEMPLATE[category].format(**NODE_HEADINGS))
        self.node_headings = "   ".join(node_heading_segments)

    def print_stats(self):
        # just run forever until ctrl-c
        while True:
            cluster_segments = []
            cluster_health = self.es_client.cluster.health()
            nodes_stats = self.es_client.nodes.stats(human=True)
            self.active_master = self.es_client.cat.master(h="id").strip() # needed to remove trailing newline

            # Print cluster health
            cluster_health['timestamp'] = self.thetime()
            status = cluster_health['status']
            for category in self.cluster_categories:
                cluster_segments.append(CLUSTER_TEMPLATE[category].format(**cluster_health))
                cluster_health_formatted = "   ".join(cluster_segments)
            print self.colorize(self.cluster_headings, ESColors.GRAY)
            print self.colorize(cluster_health_formatted, self.STATUS_COLOR[status])

            # Nodes can join and leave cluster with each iteration -- in order to report on nodes
            # that have left the cluster, maintain a list grouped by role.
            current_nodes_count = len(self.nodes_list)
            if current_nodes_count == 0:
                # First run, so we need to build the list of nodes by role
                for node_id in nodes_stats['nodes']:
                    self.nodes_list.append(node_id)
                    self.node_names[node_id] = nodes_stats['nodes'][node_id]['name']
                    node_role = self.get_role(node_id, nodes_stats)
                    self.nodes_by_role.setdefault(node_role, []).append(node_id)
            else:
                # Check for new nodes that have joined the cluster
                self.new_nodes = list(set(nodes_stats['nodes']) - set(self.nodes_list))
                if len(self.new_nodes) > 0:
                    # At least one new node id found, so add to the list
                    for node_id in self.new_nodes:
                        self.nodes_list.append(node_id)
                        self.node_names[node_id] = nodes_stats['nodes'][node_id]['name']
                        node_role = self.get_role(node_id, nodes_stats)
                        self.nodes_by_role.setdefault(node_role, []).append(node_id)

            # Print node stats
            print self.colorize(self.node_headings, ESColors.GRAY)
            for role in self.nodes_by_role:
                self.process_role(role, nodes_stats)
            print "" # space out each run for readability
            time.sleep(self.sleep_interval)


def main():
    # get command line input
    description = 'Elasticstat is a utility for real-time performance monitoring of an Elasticsearch cluster from the command line'
    parser = ESArgParser(description=description, add_help=False)

    parser.add_argument('-h',
                        '--host',
                        default='localhost',
                        dest='hostlist',
                        help='Host in Elasticsearch cluster (or a comma-delimited list of hosts)')
    parser.add_argument('--port',
                        dest='port',
                        default=9200,
                        help='HTTP Port (or include as host:port in HOSTLIST)')
    parser.add_argument('-u',
                        '--username',
                        dest='username',
                        default=None,
                        help='Username')
    parser.add_argument('-p',
                        '--password',
                        dest='password',
                        nargs='?',
                        const='PROMPT',
                        default=None,
                        help='Password (if USERNAME is specified but not PASSWORD, will prompt for password)')
    parser.add_argument('--ssl',
                        dest='use_ssl',
                        default=False,
                        action='store_true',
                        help='Connect using TLS/SSL')
    parser.add_argument('-c',
                        '--categories',
                        dest='categories',
                        default='all',
                        metavar='CATEGORY',
                        nargs='+',
                        help='Statistic categories to show [all or choose from {0}]'.format(', '.join(CATEGORIES[1:])))
    parser.add_argument('-t',
                        '--threadpools',
                        dest='threadpools',
                        default=DEFAULT_THREAD_POOLS,
                        metavar='THREADPOOL',
                        nargs='+',
                        help='Threadpools to show')
    parser.add_argument('-C',
                        '--no-color',
                        dest='no_color',
                        action='store_true',
                        default=False,
                        help='Display without ANSI color output')
    parser.add_argument('--no-pending-tasks',
                        dest='no_pending_tasks',
                        default=False,
                        help='Disable display of pending tasks in cluster health (use for Elasticsearch <v1.5)')
    parser.add_argument('delay_interval',
                        default='1',
                        nargs='?',
                        type=int,
                        metavar='DELAYINTERVAL',
                        help='How long to delay between updates, in seconds')

    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda signum, frame: sys.exit())
    elasticstat = Elasticstat(args)
    elasticstat.format_headings()
    elasticstat.print_stats()


if __name__ == "__main__":
    main()
