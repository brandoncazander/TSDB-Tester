TSDB-Tester
===========

Performance tests InfluxDB, KairosDB, and OpenTSDB

###Usage
```
usage: multiprocess_stress.py [-h] -e ENGINE -a API -amt AMOUNT -t THREADS -c CONNECTIONS remote port

positional arguments:
  remote                IP of remote host
  port                  Port of remote host

optional arguments:
  -h, --help            show this help message and exit
  -e ENGINE, --engine ENGINE
                        influxdb | opentsdb | kairosdb
  -a API, --api API     telnet | http
  -amt AMOUNT, --amount AMOUNT
                        number of metrics to send
  -t THREADS, --threads THREADS
                        number of threads to use; must be the same as number
                        of csv files in data/ directory
  -c CONNECTIONS, --connections CONNECTIONS
                        number of sockets to open
```

###Dependencies
requests==2.3.0
termcolor==1.1.0
wsgiref==0.1.2
