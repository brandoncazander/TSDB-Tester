import sys
import argparse
import socket
from socket import error as SocketError
from multiprocessing import Process, Queue
import requests
import json
import random
import time
import metrics
import select
from sys import stdout
from termcolor import colored

#Time Constants
NOW = 1404776380
ONE_YEAR_AGO = 1404776380 - 31557600
CONN_DELAY = 1
METRICS = metrics.metrics


class MetricPusher(object):
    """Stress-tests the specified storage engine by pushing as many randomly-
    generated metrics as possible over telnet or HTTP API
    """
    def __init__(self, engine, api, amount, threads, conns, remote, port):
        self.amount = amount
        self.api = api
        self.engine = engine
        self.threads = threads
        self.conns = conns
        self.remote = remote
        self.port = port
        self.suffix = "http_api_test"

        self.open_files = []

        # Check OS type
        self.os = sys.platform

        if self.os == 'linux2':
            self.epoll = select.epoll()
        elif self.os == 'darwin':
            self.kq = select.kqueue()

        self.metrics = METRICS

        self.per_thread_count = self.amount / self.threads

        self.max = 0

    def print_status(self, numbers):
        """ Print status line and percentage bar for each process """
        sys.stdout.write('\033[2J\033[H')
        total_count = 0
        total_rate = 0
        for process_num, tuple in numbers.items():
            percent = (tuple[0] / float(self.per_thread_count))
            bar = ('=' * int(percent * 30)).ljust(30)
            count_msg = "Process %d: %7d/%d  (%5d metrics/sec) [%s]%2d%%\n" % (process_num,
                                                                               tuple[0],
                                                                               self.per_thread_count,
                                                                               tuple[1],
                                                                               colored(bar, 'blue'),
                                                                               percent*100)
            stdout.write(count_msg)
            total_count += tuple[0]
            total_rate += tuple[1]
        stdout.write("    Total: %7d/%d (%6d metrics/sec)\n" % (total_count, self.amount, total_rate))
        if total_rate > self.max:
            self.max = total_rate
        stdout.flush()

    def _setup(self):
        """
        Open files
        Create threads
        Open sockets and register with epoll
        Start threads and call _send on each thread
        """
        status = Queue()
        workers = []
        numbers = {}

        if self.api == "telnet":

            # Threads
            if self.os == 'linux2':
                for thread_num in range(0, self.threads):
                    # Open sockets for this thread
                    open_sockets = {}
                    for num in range(0, self.conns):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        socket_fileno = sock.fileno()

                        self.epoll.register(socket_fileno,
                                            select.EPOLLOUT)

                        open_sockets[socket_fileno] = sock
                        print "Connecting to %s on port %s" % (self.remote,
                                                               self.port)
                        open_sockets[socket_fileno].connect((self.remote,
                                                             self.port))

                    # Start this process
                    print "Starting process #%s" % thread_num
                    p = Process(target=self._send, args=(open_sockets, thread_num, status))
                    p.start()
                    workers.append(p)
                    numbers[thread_num] = (0, 0)

                while any(i.is_alive() for i in workers):
                    time.sleep(0.1)
                    while not status.empty():
                        process, s_count, s_rate = status.get()
                        numbers[process] = (s_count, s_rate)
                        self.print_status(numbers)

            elif self.os == 'darwin':
                for thread_num in range(0, self.threads):
                    # Open sockets for this thread
                    open_sockets = {}
                    for num in range(0, self.conns):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        socket_fileno = sock.fileno()

                        open_sockets[socket_fileno] = sock
                        print "Connecting to %s on port %s" % (self.remote,
                                                               self.port)
                        open_sockets[socket_fileno].connect((self.remote,
                                                             self.port))

                    # Start this process
                    print "Starting process #%s" % thread_num
                    p = Process(target=self._send, args=(open_sockets, thread_num, status))
                    p.start()
                    workers.append(p)
                    numbers[thread_num] = (0, 0)

                while any(i.is_alive() for i in workers):
                    time.sleep(0.1)
                    while not status.empty():
                        process, s_count, s_rate = status.get()
                        numbers[process] = (s_count, s_rate)
                        self.print_status(numbers)

        elif self.api == "http":
            for num in range(0, self.threads):
                # Start this process
                print "Starting process #%s" % num
                p = Process(target=self._send, args=(None, ))
                p.start()

    def _send(self, open_sockets, process, status):
        """Send over the open sockets"""
        count = 0
        last_time = time.time()

        if self.os == 'darwin':
            kq = select.kqueue()
            ev = []
            for sock in open_sockets:
                ev.append(select.kevent(sock,
                          select.KQ_FILTER_WRITE,
                          select.KQ_EV_ONESHOT | select.KQ_EV_ADD | select.KQ_EV_ENABLE))

        if self.api == "telnet":

            try:
                while True:
                    # Get our epoll events
                    if self.os == 'linux2':
                        events = self.epoll.poll(5)
                        for fileNum, event in events:
                            if fileNum in open_sockets:
                                count += 1
                                if count % 5000 == 0:
                                    time_delta = time.time() - last_time
                                    # Send the current stats to the queue
                                    status.put([process, count, int(count / time_delta)])

                                # Make a new metric
                                metric = self.metrics[random.randint(0, len(self.metrics)-1)]
                                metric_time = int(random.randint(ONE_YEAR_AGO, NOW))
                                amount = random.randint(0, 1000000)
                                tag = "stressTest"

                                # InfluxDB requires a different format and doesn't support tags
                                if self.engine == "influxdb":
                                    # collectd_test_01.memory.memory.cached.value 2335620000 1404405000
                                    message = "%s.%s %s %s\n" % (tag, metric, amount, metric_time)

                                # OpenTSDB and KairosDB are pretty similar though
                                else:
                                    # put memory.memory.cached.value 1404405000000 2335620000 host=collectd_test_01
                                    message = "put %s %s %s host=%s\n" % (metric, metric_time*1000, amount, tag)

                                # Send message
                                try:
                                    data = open_sockets[fileNum].send(message)
                                except SocketError:
                                    # Stop watching this socket
                                    self.epoll.modify(fileNum, 0)

                    elif self.os == 'darwin':

                        revents = kq.control(ev, 1, None)
                        for event in revents:
                            if event.filter == select.KQ_FILTER_WRITE:
                                count += 1
                                if count % 5000 == 0:
                                    time_delta = time.time() - last_time
                                    # Send the current stats to the queue
                                    status.put([process, count, int(count / time_delta)])

                                # Make a new metric
                                metric = self.metrics[random.randint(0, len(self.metrics)-1)]
                                metric_time = int(random.randint(ONE_YEAR_AGO, NOW))
                                amount = random.randint(0, 1000000)
                                tag = "stressTest"

                                # InfluxDB requires a different format and doesn't support tags
                                if self.engine == "influxdb":
                                    # collectd_test_01.memory.memory.cached.value 2335620000 1404405000
                                    message = "%s.%s %s %s\n" % (tag, metric, amount, metric_time)

                                # OpenTSDB and KairosDB are pretty similar though
                                else:
                                    # put memory.memory.cached.value 1404405000000 2335620000 host=collectd_test_01
                                    message = "put %s %s %s host=%s\n" % (metric, metric_time*1000, amount, tag)

                                # Send message
                                try:
                                    data = open_sockets[event.ident].send(message)
                                except SocketError:
                                    # Stop watching this socket
                                    pass

                    # Stop sending when we reach limit of metrics specified
                    if (count*self.threads) == self.amount:
                        # Break out of while loop
                        time_delta = time.time() - last_time
                        status.put([process, count, int(count / time_delta)])
                        break

            finally:
                for client_socket in open_sockets:
                    # Should probably clean up sockets here
                    pass

        elif self.api == "http":

            if self.engine == "influxdb":
                self.remote = 'localhost'
                self.url = 'http://localhost:8086/db/graphite/series?u=brandon&p=password'

            elif self.engine == "kairosdb":
                self.remote = 'localhost'
                self.url = 'http://localhost:8080/api/v1/datapoints'

            while True:

                # Read the next 100 lines in the csv file
                count += 1
                data = file.readline().split(", ")
                if len(data) is not 4:
                    return 0
                metric = data[0]
                metric_time = data[1][:10]
                amount = data[2]
                tag = data[3].rstrip('\n')

                if self.engine == "kairosdb":
                    payload = [
                        {
                            'name': metric+self.suffix,
                            'timestamp': int(metric_time) * 1000,
                            'value': amount,
                            'tags': {
                                'host': tag
                            }
                        }
                    ]
                elif self.engine == "influxdb":
                    payload = [
                        {
                            'name': metric+self.suffix,
                            'columns': ['time', 'value'],
                            'points': [
                                [int(metric_time) * 1000, amount]
                            ]
                        }
                    ]

                requests.post(self.url, data=json.dumps(payload))

                # Stop sending when we reach limit of metrics specified
                if count > self.amount:
                    # Break out of while loop
                    break

    """
    Main run function
    """
    def run(self):
        self._setup()
        print "Max rate: %d metrics/sec" % self.max


def main():
    """Parse arguments and run program"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--engine", help="influxdb | opentsdb | kairosdb", required=True)
    parser.add_argument("-a", "--api", help="telnet | http", required=True)
    parser.add_argument("-amt", "--amount", help="number of metrics to send", type=int, required=True)
    parser.add_argument("-t", "--threads", help="number of threads to use; must be the same as number "
                                                "of csv files in data/ directory", type=int, required=True)
    parser.add_argument("-c", "--connections", help="number of sockets to open", type=int, required=True)
    parser.add_argument("remote", help="IP of remote host")
    parser.add_argument("port", help="Port of remote host", type=int)
    args = parser.parse_args()

    mp = MetricPusher(args.engine, args.api, args.amount, args.threads, args.connections, args.remote, args.port)
    mp.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
