import sys
import argparse
import time
import socket
import requests
import json
import select
import io


class MetricPusher(object):
    def __init__(self, type, amount, engine, conns, remote, port):
        self.type = type
        self.amount = amount
        self.api = "telnet"
        self.engine = engine
        self.conns = conns
        self.remote = remote
        self.port = port

    def _data(self):
        data = open('data/metrics.csv', 'r')
        return data

    """
    Open connection to InfluxDB on local server
    """
    def _record_conn_start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.remote, self.port))

    """
    Send count of data points sent
    """
    def _record_send(self, test, count):
        now = int(time.time())
        message = "%s %s %s\n" % (test, count, now)

        self.sock.send(message)

    """
    Close
    """
    def _record_conn_end(self):
        self.sock.close()

    """
    Main run function
    """
    def run(self):

        """
        Push as many metrics as we can
        """
        if self.type == "push":

            """
            Open the data file
            """
            metrics = io.open('data/random_metrics.csv', 'r')

            """
            telnet
            """
            if self.api == "telnet":

                # self._record_conn_start()

                # Set up epoll
                epoll = select.epoll()

                sockets = {}

                """
                Sockets to remote server
                """
                for num in range(1, self.conns):
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sockets[sock.fileno()] = sock
                    print "Connecting to %s on port %s" % (self.remote, self.port)
                    sockets[sock.fileno()].connect((self.remote, self.port))
                    epoll.register(sock.fileno(), select.EPOLLOUT)

                count = 0

                try:
                    while True:
                        events = epoll.poll(5)

                        for fileNum, event in events:
                            if fileNum in sockets:
                                count += 1
                                data = metrics.readline().split(", ")
                                if len(data) is not 4:
                                    return 0
                                metric = data[0]
                                time = data[1][:10]
                                amount = data[2]
                                tag = data[3].rstrip('\n')

                                if self.engine == "influxdb":
                                    # collectd_test_01.memory.memory.cached.value 2335620000 1404405000
                                    message = "%s.%s %s %s\n" % (tag, metric, amount, time)
                                else:
                                    # put memory.memory.cached.value 1404405000000 2335620000 host=collectd_test_01
                                    message = "put %s %s %s host=%s\n" % (metric, time, amount, tag)

                                # self._record_send(("%s.count" % self.engine), count)

                                # Send message
                                sockets[fileNum].send(message)

                        if count > self.amount:
                            break

                finally:
                    for client_socket in sockets:
                        # epoll.unregister(client_socket)
                        # epoll.close()
                        pass

                # for line in self._data():
                #     count += 1
                #     data = line.split(", ")
                #     metric = data[0]
                #     time = data[1]
                #     amount = data[2]
                #     tag = data[3].rstrip('\n')
                #
                #     """
                #     InfluxDB doesn't support tagging so append tag to beginning
                #     """
                #     if self.engine == "influxdb":
                #         # put collectd_test_01.memory.memory.cached.value 1404405000000 2335620000
                #         message = "put %s.%s %s %s\n" % (tag, metric, time, amount)
                #     else:
                #         # put memory.memory.cached.value 1404405000000 2335620000 host=collectd_test_01
                #         message = "put %s %s %s host=%s\n" % (metric, time, amount, tag)
                #
                #     self._record_send(("%s.count" % self.engine), count)
                #
                #     # Send message
                #     sock.send(message)

                # sock.close()
                # self._record_conn_end()

            """
            http api
            """
            if self.api == "api":
                self._record_conn_start()
                count = 0

                address = 'http://%s:%s/' % (self.remote, self.port)

                if self.engine == "influxdb":

                    for line in self._data():
                        count += 1
                        data = line.split(", ")
                        metric = data[0]
                        time = data[1]
                        amount = data[2]
                        tag = data[3].rstrip('\n')

                        message = [
                            {
                                "name": ("%s.%s" % (tag, metric)),
                                "columns": ["time", "point"],
                                "points": [
                                    [time, amount]
                                ]
                            }
                        ]
                        address += '/db/performance_test-%s-%s/series' % (self.type, self.api)

                        r = requests.post(address, auth=('brandon', 'password'), data=json.dumps(message))
                        print r
                        self._record_send(("%s.count" % self.engine), count)

                self._record_conn_end()

            metrics.close()

        """
        Query as much data as we can
        """
        if self.type == "query":
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--type", help="push | query")
    # parser.add_argument("-a", "--api", help="telnet | api")
    parser.add_argument("-amt", "--amount", help="number of metrics to send", type=int)
    parser.add_argument("-e", "--engine", help="influxdb | opentsdb | kairosdb")
    parser.add_argument("-c", "--connections", help="number of sockets to open", type=int)
    parser.add_argument("remote", help="IP of remote host")
    parser.add_argument("port", help="Port of remote host", type=int)
    args = parser.parse_args()

    mp = MetricPusher(args.type, args.amount, args.engine, args.connections, args.remote, args.port)
    mp.run()

    return 0

if __name__ == "__main__":
    sys.exit(main())
