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
NODES_TEMPLATE = """{name:24} {role:<6} {load_avg:>18}   {used_mem:>4} {used_heap:>4}  {old_gc_sz:8} {old_gc:8} {young_gc:8}   {index_threads:<8} {search_threads:<8} {bulk_threads:<8} {get_threads:<8} {merge_threads:<8} {fielddata:^7}   {http_conn:>6} {transport_conn:>6}   {merge_time:>8} {store_throttle:>8}  {docs}"""
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
THREAD_POOLS = ["index", "search", "bulk", "get", "merge"]

class ESArgParser(argparse.ArgumentParser):
    """ArgumentParser which prints help by default on any arg parsing error"""
    def error(self, message):
        self.print_help()
        sys.exit(2)
        
class ElasticStat:
    """ElasticStat Utility Class"""
    
    def __init__(self, host, port, username, password, delay_interval):

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
        
        # 
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
            # uh, wat? no idea if we get to here
            return "UNK"
        
    def get_gc_stats(self, node_name, node_gc_stats):
        # check if this is a new node
        if node_name not in self.node_counters['gc']:
            # new so init counters and return no data
            self.node_counters['gc'][node_name] = {'old': 0, 'young': 0}
            self.node_counters['gc'][node_name]['old'] = node_gc_stats['old']['collection_count']
            self.node_counters['gc'][node_name]['young'] = node_gc_stats['young']['collection_count']
            return("-|-", "-|-")
        else:
            # existing node, so calculate the new deltas, update counters, and return results
            old_gc_count = node_gc_stats['old']['collection_count']
            young_gc_count = node_gc_stats['young']['collection_count']
            old_gc_delta = old_gc_count - self.node_counters['gc'][node_name]['old']
            young_gc_delta = young_gc_count - self.node_counters['gc'][node_name]['young']
            self.node_counters['gc'][node_name]['old'] = old_gc_count
            self.node_counters['gc'][node_name]['young'] = young_gc_count
            old_gc_results = "{0}|{0}ms".format(old_gc_delta, node_gc_stats['old']['collection_time_in_millis'])
            young_gc_results = "{0}|{0}ms".format(young_gc_delta, node_gc_stats['young']['collection_time_in_millis'])
            return(old_gc_results, young_gc_results)
    
    def get_fd_stats(self, node_name, current_evictions, current_tripped):
        # check if this is a new node
        if node_name not in self.node_counters['fd']:
            # new so init counters and return no data
            self.node_counters['fd'][node_name] = {'fde': 0, 'fdt': 0}
            self.node_counters['fd'][node_name]['fde'] = current_evictions
            self.node_counters['fd'][node_name]['fdt'] = current_tripped
            return("-|-")
        else:
            # existing node, so calc new deltas, update counters, and return results
            fde_delta = current_evictions - self.node_counters['fd'][node_name]['fde']
            self.node_counters['fd'][node_name]['fde'] = current_evictions
            fdt_delta = current_tripped - self.node_counters['fd'][node_name]['fdt']
            self.node_counters['fd'][node_name]['fdt'] = current_tripped
            return("{0}|{1}".format(fde_delta, fdt_delta))
        
    def get_http_conns(self, node_name, http_conns):
        # check if this is a new node
        if node_name not in self.node_counters['hconn']:
            self.node_counters['hconn'][node_name] = http_conns['total_opened']
            return ("{0}|-".format(http_conns['current_open']))
        else:
            open_delta = http_conns['total_opened'] - self.node_counters['hconn'][node_name]
            self.node_counters['hconn'][node_name] = http_conns['total_opened']
            return("{0}|{1}".format(http_conns['current_open'], open_delta))

    def process_node(self, role, node_id, node):
        processed_node = {}
        processed_node['name'] = node['name']
        processed_node['role'] = role
        if self.active_master == node_id:
            # Flag active master in role column
            processed_node['role'] += "*"
        if node_id in self.new_nodes:
            # Flag that this is a node that joined the cluster this round
            processed_node['name'] = "+" + processed_node['name']
            
        # Load / mem / heap
        processed_node['load_avg'] = "/".join(str(x) for x in node['os']['load_average'])
        processed_node['used_mem'] = "{0}%".format(node['os']['mem']['used_percent'])
        processed_node['used_heap'] = "{0}%".format(node['jvm']['mem']['heap_used_percent'])
        
        # GC counters
        processed_node['old_gc_sz'] = node['jvm']['mem']['pools']['old']['used']
        node_gc_stats = node['jvm']['gc']['collectors']
        processed_node['old_gc'], processed_node['young_gc'] = self.get_gc_stats(processed_node['name'], node_gc_stats)
        
        # Threads
        for pool in THREAD_POOLS:
            processed_node[pool + '_threads'] = "{0}|{1}|{2}".format(node['thread_pool'][pool]['active'],
                                                                  node['thread_pool'][pool]['queue'],
                                                                  node['thread_pool'][pool]['rejected'])
        
        # Field data evictions | circuit break trips
        processed_node['fielddata'] = self.get_fd_stats(processed_node['name'],
                                                     node['indices']['fielddata']['evictions'],
                                                     node['breakers']['fielddata']['tripped'])    
        
        # Connections
        processed_node['http_conn'] = self.get_http_conns(processed_node['name'],
                                                   node['http'])
        processed_node['transport_conn'] = node['transport']['server_open']
        
        # Misc
        if role in ['DATA', 'ALL']:
            processed_node['merge_time'] = node['indices']['merges']['total_time']
            processed_node['store_throttle'] = node['indices']['store']['throttle_time']
            processed_node['docs'] = "{0}|{1}".format(node['indices']['docs']['count'],
                                                   node['indices']['docs']['deleted'])
        else:
            processed_node['merge_time'] = "-"
            processed_node['store_throttle'] = "-"
            processed_node['docs'] = "-|-"
        
        return(NODES_TEMPLATE.format(**processed_node))
            
    def process_role(self, role, nodes_stats):
        procs = []
        for node_id in self.nodes_by_role[role]:
            if node_id not in nodes_stats['nodes']:
                # did not get any data on this node, likely it left the cluster
                # ...however it may have re-joined the cluster under a new node_id (such as a node restart)
                failed_node_name = self.node_names[node_id]
                new_nodes_by_name = {nodes_stats['nodes'][id]['name']: id for id in self.new_nodes}
                if failed_node_name in new_nodes_by_name:
                    # ...found it!  Remove the old node_id, we've already added the new node at this point
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
                
    def printStats(self):
        counter = 0

        # just run forever until ctrl-c
        while True:
            cluster_health = self.es_client.cluster.health()
            nodes_stats = self.es_client.nodes.stats(human=True)
            self.active_master = self.es_client.cat.master(h="id").strip() # needed to remove trailing newline

            # Print cluster health
            cluster_health['timestamp'] = self.thetime()
            print CLUSTER_TEMPLATE.format(**CLUSTER_HEADINGS)
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
            print NODES_TEMPLATE.format(**NODE_HEADINGS)
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
    parser.add_argument('delay_interval',
                        default='1',
                        nargs='?',
                        type=int,
                        metavar='DELAYINTERVAL',
                        help='How long to delay between checks')

    args = parser.parse_args()

    signal.signal(signal.SIGINT, lambda signum, frame: sys.exit())
    elasticstat = ElasticStat(args.hostlist, args.port, args.username, args.password, args.delay_interval)
    elasticstat.printStats()


if __name__ == "__main__":
    main()
