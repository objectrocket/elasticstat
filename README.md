# Elasticstat

usage: elasticstat.py [-h HOSTLIST] [--port PORT] [-u USERNAME]
                      [-p [PASSWORD]] [--ssl] [-c CATEGORY [CATEGORY ...]]
                      [-t THREADPOOL [THREADPOOL ...]] [-C]
                      [DELAYINTERVAL]

Elasticsearch command line metrics

positional arguments:
  DELAYINTERVAL         How long to delay between checks

optional arguments:
  -h HOSTLIST, --host HOSTLIST
                        Host in Elasticsearch cluster (or a comma-delimited
                        list of hosts)
  --port PORT           HTTP Port (or include as host:port in HOSTLIST)
  -u USERNAME, --username USERNAME
                        Username
  -p [PASSWORD], --password [PASSWORD]
                        Password
  --ssl                 Connect using TLS/SSL
  -c CATEGORY [CATEGORY ...], --categories CATEGORY [CATEGORY ...]
                        Statistic categories to show [all or choose from os,
                        jvm, threads, fielddata, connections, data_nodes]
  -t THREADPOOL [THREADPOOL ...], --threadpools THREADPOOL [THREADPOOL ...]
                        Thread pools to show
  -C, --no-color        Display without color output
