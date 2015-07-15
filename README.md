# Elasticstat

Written By: Jeff Tharp, ObjectRocket by Rackspace
http://objectrocket.com/elasticsearch

## Description
Elasticstat is a utility for real-time performance monitoring of an Elasticsearch cluster from the command line,
much like how the Unix utilities iostat or vmstat work.  The frequency of updates can be controled via the DELAYINTERVAL
 optional parameter, which specifies a delay in seconds after each update.

Performance metrics shown are based on the articles 
[Cluster Health](https://www.elastic.co/guide/en/elasticsearch/guide/current/_cluster_health.html) and 
[Monitoring Individual Nodes](https://www.elastic.co/guide/en/elasticsearch/guide/current/_monitoring_individual_nodes.html)
from the Elasticsearch Definitive Guide.  Please refer to these articles for further insight as to the significance of each
metric.

## Requirements

- python 2.6+
- [elasticsearch-py](http://elasticsearch-py.rtfd.org/)
- Access to an Elasticsearch 1.5.0+ cluster you wish to monitor (via either HTTP or HTTPS)

## Install

Install `elasticstat` via pip:

```
pip install elasticstat
```

## Usage

```
elasticstat [-h HOSTLIST] [--port PORT] [-u USERNAME]
			[-p [PASSWORD]] [--ssl] [-c CATEGORY [CATEGORY ...]]
			[-t THREADPOOL [THREADPOOL ...]] [-C]
            [DELAYINTERVAL]

Elasticstat is a utility for real-time performance monitoring of an Elasticsearch cluster from the command line

positional arguments:
  DELAYINTERVAL         How long to delay between updates, in seconds

optional arguments:
  -h HOSTLIST, --host HOSTLIST
                        Host in Elasticsearch cluster (or a comma-delimited
                        list of hosts from the same cluster)
  --port PORT           HTTP Port (or include as host:port in HOSTLIST)
  -u USERNAME, --username USERNAME
                        Username
  -p [PASSWORD], --password [PASSWORD]
                        Password (if USERNAME is specified but not PASSWORD,
                        will prompt for password)
  --ssl                 Connect using TLS/SSL
  -c CATEGORY [CATEGORY ...], --categories CATEGORY [CATEGORY ...]
                        Statistic categories to show [all or choose from os,
                        jvm, threads, fielddata, connections, data_nodes]
  -t THREADPOOL [THREADPOOL ...], --threadpools THREADPOOL [THREADPOOL ...]
                        Threadpools to show
  -C, --no-color        Display without ANSI color output
```

## Cluster-level Metrics

- cluster: the name of the cluster
- status: the familiar green/yellow/red status of the cluster - yellow indicates at least one replica shard is unavailable, red indicates at least one primary shard is unavailable.
- shards: total number of active primary and replica shards across all indices
- pri: the number of active / allocated primary shards across all indices
- relo: number of shards currently relocating from one data node to another
- init: number of shards being freshly created
- unassign: number of shards defined in an index but not allocated to a data node
- pending tasks: the number of tasks pending (see [Pending Tasks](https://www.elastic.co/guide/en/elasticsearch/guide/current/_pending_tasks.html))
- time: current local time for this update

## Node-level Metrics

- general
  - node: node name, typically a shortened version of the hostname of the node
  - role: the [role](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-node.html) of this node in the cluster as follows:
    - ALL: a node serving as both a master and data node (Elasticsearch's default role) -- node.master = true, node.data = true
    - DATA: a data-only node, node.master = false, node.data = true
    - MST: a master-only node, node.master = true, node.data = false -- the active cluster master is marked with an '*'
    - RTR: a client node, node.master = false, node.data = false
    - UNK: node with an unkown or undetermined role
- os
  - load: the 1 minute / 5 minute / 15 minute [load average](http://blog.scoutapp.com/articles/2009/07/31/understanding-load-averages) of the node
  - mem: percentage of total memory used on the node (including memory used by the kernel and other processes besides Elasticsearch)
- [jvm](https://www.elastic.co/guide/en/elasticsearch/guide/current/_monitoring_individual_nodes.html#_jvm_section)
  - heap: percentage of Java heap memory in use.  Java garbage collections occur when this reaches or exceeds 75%.
  - old sz: total size of the memory pool for the old generation portion of the Java heap
  - old gc: number of garbage collection events that have occured, and their cumulative time since the last update, for the old generation region of Java heap
  - young gc: number of garbage collection events that have occured, and their cumulative time since the last update, for the young (aka eden) generation region of Java heap
- threads ([threadpools](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-threadpool.html)): number of active | queued | rejected threads for each threadpool.  Default threadpools listed are as follows:
  - index: (non-bulk) indexing requests
  - search: all search and query requests
  - bulk: bulk requests
  - get: all get-by-ID operations
  - merge: threadpool for managing Lucene merges
- [fielddata](https://www.elastic.co/guide/en/elasticsearch/guide/current/_limiting_memory_usage.html#fielddata-size)
  - fde: count of field data evictions that have occurred since last update
  - fdt: number of times the field data circuit breaker has tripped since the last update
- connections
  - hconn: number of active HTTP/HTTPS connections to this node (REST API)
  - tconn: number of active transport connections to this node (Java API, includes intra-cluster node-to-node connections)
- data_nodes: metrics useful only for data-bearing nodes
  - merges: total time spent in Lucene segment merges since the last time the node was restarted
  - idx st: [index store throttle](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-store.html#store-throttling), the total time indexing has been throttled to a single thread since the last time the node was restarted (see [Segments and Merging](https://www.elastic.co/guide/en/elasticsearch/guide/current/indexing-performance.html#segments-and-merging))
  - docs: the total number of documents in all index shards allocated to this node.  If there is a second number, this is the total number of deleted documents not yet merged

## License

Copyright 2015 Rackspace US, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.