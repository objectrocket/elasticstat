#!/usr/bin/env python

import argparse
import datetime
import getpass
import signal
import sys
import time

from elasticsearch import Elasticsearch

# cluster_name status shards pri relo init unassign pending_tasks timestamp
CLUSTER_TEMPLATE = """{cluster_name:33} {status:6} {active_shards:>6} {active_primary_shards:>4} {relocating_shards:>4} {initializing_shards:>4} {unassigned_shards:>8} {number_of_pending_tasks:>13}  {timestamp:8}"""
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

# node_name role load_avg mem% heap%  old sz old gc young gc
NODES_TEMPLATE = {}
NODES_TEMPLATE['general'] = """{name:24} {role:<6}"""
NODES_TEMPLATE['os'] = """{load_avg:>18} {used_mem:>4}"""
NODES_TEMPLATE['jvm'] = """{used_heap:>4}  {old_gc_sz:8} {old_gc:8} {young_gc:8}"""
NODES_TEMPLATE['threads'] = """{threads:<8}"""
NODES_TEMPLATE['fielddata'] = """{fielddata:^7}"""
NODES_TEMPLATE['connections'] = """{http_conn:>6} {transport_conn:>6}"""
NODES_TEMPLATE['data_nodes'] = """{merge_time:>8} {store_throttle:>8}  {docs}"""
NODES_FAILED_TEMPLATE = """{name:24} {role:<6} (No data received, node may have left cluster)"""
NODE_HEADINGS = {}
NODE_HEADINGS["name"] = "nodes"
NODE_HEADINGS["role"] = "role"
NODE_HEADINGS["load_avg"] = "load"
NODE_HEADINGS["used_mem"] = "mem"
NODE_HEADINGS["used_heap"] = "heap"
NODE_HEADINGS["old_gc_sz"] = "old sz"
NODE_HEADINGS["old_gc"] = "old gc"
NODE_HEADINGS["young_gc"] = "young gc"
NODE_HEADINGS["index_threads"] = "index"
NODE_HEADINGS["bulk_threads"] = "bulk"
NODE_HEADINGS["get_threads"] = "get"
NODE_HEADINGS["search_threads"] = "search"
NODE_HEADINGS["merge_threads"] = "merge"
NODE_HEADINGS["fielddata"] = "fde|fdt"
NODE_HEADINGS["http_conn"] = "hconn"
NODE_HEADINGS["transport_conn"] = "tconn"
NODE_HEADINGS["merge_time"] = "merges"
NODE_HEADINGS["store_throttle"] = "idx st"
NODE_HEADINGS["docs"] = "docs"
DEFAULT_THREAD_POOLS = ["index", "search", "bulk", "get", "merge"]
CATEGORIES = ['general', 'os', 'jvm', 'threads', 'fielddata', 'connections', 'data_nodes']

class ESArgParser(argparse.ArgumentParser):
    """ArgumentParser which prints help by default on any arg parsing error"""
    def error(self, message):
        self.print_help()
        sys.exit(2)
        
class Elasticstat:
    """Elasticstat"""
    
    def __init__(self, host, port, username, password, delay_interval, categories, threadpools):

        self.sleep_interval = delay_interval
        self.node_counters = {}
        self.node_counters['gc'] = {}
        self.node_counters['fd'] = {}
        self.node_counters['hconn'] = {}
        self.nodes_list = [] # used for detecting new nodes
        self.nodes_by_role = {} # main list of nodes, organized by role
        self.node_names = {} # node names, organized by id
        self.new_nodes = [] # used to track new nodes that join the cluster
        self.active_master = ""
        self.threadpools = threadpools
        
        # categories for display
        if categories == 'all':
            self.categories = CATEGORIES
        else:
            self.categories = ['general'] + categories
        
        # check for port in host
        if ':' in host:
            host, port = host.split(':')
        
        host_dict = {'host': host, 'port': port}
        
        # check for auth
        if username is not None:
            if password is None or password == 'PROMPT':
                password = getpass.getpass()
            host_dict['http_auth'] = (username, password)
        
        self.es_client = Elasticsearch([host_dict])

    def thetime(self):
        return datetime.datetime.now().strftime("%H:%M:%S")
    
    def get_role(self, attributes):
        # This is dumb, but if data/master is true, ES doesn't include the key in 
        # the attributes subdoc.  Why?? :-P
        ismaster = 'true'
        isdata = 'true'
        
        if 'data' in attributes:
            isdata = attributes['data']
        if 'master' in attributes:
            ismaster = attributes['master']
            
        if ismaster == 'true' and isdata == 'true':
            # if is both master and data node, client is assumed as well
            return "ALL"
        elif ismaster == 'true' and isdata == 'false':
            # master node
            return "MST"
        elif ismaster == 'false' and isdata == 'true':
            # data-only node
            return "DATA"
        elif ismaster == 'false' and isdata == 'false':
            # client node (using RTR like monogostat)
            return "RTR"
        else:
            # uh, wat? no idea if we reach here
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
        return(NODES_TEMPLATE['os'].format(load_avg="/".join(str(x) for x in node['os']['load_average']),
                                           used_mem="{0}%".format(node['os']['mem']['used_percent'])))
    
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
        return(self.get_fd_stats(node_id,
                                 node['indices']['fielddata']['evictions'],
                                 node['breakers']['fielddata']['tripped']))
        
    def process_node_connections(self, role, node_id, node):
        processed_node_conns = {}
        processed_node_conns['http_conn'] = self.get_http_conns(node_id, node['http'])
        processed_node_conns['transport_conn'] = node['transport']['server_open']
        return(NODES_TEMPLATE['connections'].format(**processed_node_conns))
    
    def process_node_data_nodes(self, role, node_id, node):
        processed_node_dn = {}
        # Data node specific metrics
        if role in ['DATA', 'ALL']:
            processed_node_dn['merge_time'] = node['indices']['merges']['total_time']
            processed_node_dn['store_throttle'] = node['indices']['store']['throttle_time']
            processed_node_dn['docs'] = "{0}|{1}".format(node['indices']['docs']['count'],
                                                         node['indices']['docs']['deleted'])
        else:
            processed_node_dn['merge_time'] = "-"
            processed_node_dn['store_throttle'] = "-"
            processed_node_dn['docs'] = "-|-"
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
                    self.new_nodes.remove(new_node_id) # So we don't flag this as a new node visually
                    self.nodes_list.remove(node_id)
                    self.node_names.pop(node_id)
                    self.nodes_by_role[role].remove(node_id)
                else:
                    failed_node = {}
                    failed_node['name'] = "-" + failed_node_name
                    failed_node['role'] = "({0})".format(role) # Role it had when we last saw this node in the cluster
                    print NODES_FAILED_TEMPLATE.format(**failed_node)
                continue
            # make sure node's role hasn't changed
            current_role = self.get_role(nodes_stats['nodes'][node_id]['attributes'])
            if current_role != role:
                # Role changed, update lists so output will be correct on next iteration
                self.nodes_by_role.setdefault(current_role, []).append(node_id) # add to new role
                self.nodes_by_role[role].remove(node_id) # remove from current role
            print self.process_node(current_role, node_id, nodes_stats['nodes'][node_id])

    def get_threads_headings(self):
        thread_segments = []
        for pool in self.threadpools:
            thread_segments.append(NODES_TEMPLATE['threads'].format(threads=pool))
        return(" ".join(thread_segments))
                    
    def format_headings(self):
        """Format both cluster and node headings once and then store for later output"""
        node_heading_segments = []
        
        # cluster headings
        self.cluster_headings = CLUSTER_TEMPLATE.format(**CLUSTER_HEADINGS)
        
        # node headings
        for category in self.categories:
            if category == 'threads':
                node_heading_segments.append(self.get_threads_headings())
            else:
                node_heading_segments.append(NODES_TEMPLATE[category].format(**NODE_HEADINGS))
        self.node_headings = "   ".join(node_heading_segments)
        
    def print_stats(self):
        counter = 0

        # just run forever until ctrl-c
        while True:
            cluster_health = self.es_client.cluster.health()
            nodes_stats = self.es_client.nodes.stats(human=True)
            self.active_master = self.es_client.cat.master(h="id").strip() # needed to remove trailing newline

            # Print cluster health
            cluster_health['timestamp'] = self.thetime()
            print self.cluster_headings
            print CLUSTER_TEMPLATE.format(**cluster_health)
            print "" # space for readability
            
            # Nodes can join and leave cluster with each iteration -- in order to report on nodes
            # that have left the cluster, maintain a list grouped by role.
            current_nodes_count = len(self.nodes_list)
            if current_nodes_count == 0:
                # First run, so we need to build the list of nodes by role
                for node_id in nodes_stats['nodes']:
                    self.nodes_list.append(node_id)
                    self.node_names[node_id] = nodes_stats['nodes'][node_id]['name']
                    node_role = self.get_role(nodes_stats['nodes'][node_id]['attributes'])
                    self.nodes_by_role.setdefault(node_role, []).append(node_id)
            else:
                # Check for new nodes that have joined the cluster
                self.new_nodes = list(set(nodes_stats['nodes']) - set(self.nodes_list))
                if len(self.new_nodes) > 0:
                    # At least one new node id found, so add to the list
                    for node_id in self.new_nodes:
                        self.nodes_list.append(node_id)
                        self.node_names[node_id] = nodes_stats['nodes'][node_id]['name']
                        node_role = self.get_role(nodes_stats['nodes'][node_id]['attributes'])
                        self.nodes_by_role.setdefault(node_role, []).append(node_id)
               
            # Print node stats
            print self.node_headings
            for role in self.nodes_by_role:
                self.process_role(role, nodes_stats)
            print "" # space out each run for readability
            time.sleep(self.sleep_interval)


def main():
    # get command line input
    parser = ESArgParser(description='Elasticsearch command line metrics', add_help=False)

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
                        help='Password')
    parser.add_argument('-c',
                        '--categories',
                        dest='categories',
                        default='all',
                        metavar='CATEGORY',
                        nargs='+',
                        help='Statistic categories to show')
    parser.add_argument('-T',
                        '--threadpools',
                        dest='threadpools',
                        default=DEFAULT_THREAD_POOLS,
                        metavar='THREADPOOL',
                        nargs='+',
                        help='Thread pools to show')
    parser.add_argument('delay_interval',
                        default='1',
                        nargs='?',
                        type=int,
                        metavar='DELAYINTERVAL',
                        help='How long to delay between checks')

    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda signum, frame: sys.exit())
    elasticstat = Elasticstat(args.hostlist, args.port, args.username, args.password, args.delay_interval, args.categories, args.threadpools)
    elasticstat.format_headings()
    elasticstat.print_stats()


if __name__ == "__main__":
    main()
