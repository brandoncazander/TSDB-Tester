import sys
import argparse
import timeit
import requests
import json
import random
import metrics

metrics = metrics.metrics

#Defaults for metrics to cover one year
now = 1404776380
one_year_ago = 1404776380 - 31557600


class MetricGrabber(object):
    """Gets the specified amount of data points via HTTP API
    for all three storage engines.
    """
    def __init__(self, amount):
        self.amount = amount
        self.points = 0

    def runTest(self):
        if self.engine == "influxdb":
            self.remote = 'localhost'
            self.url = 'http://locahost:8086/db/graphite/series'
            payload = {
                'u': 'root',
                'p': 'root',
                'q': 'select * from random_host_1.cpu-0.cpu-idle'
            }
            r = requests.get(self.url, params=payload)
            resp = r.json()
            self.points = 28202 * self.amount

        elif self.engine == "opentsdb":
            self.remote = 'localhost'
            self.url = 'http://localhost:4242/api/query'
            payload = {
                'start': 1404545541,
                'end': 1404755263,
                'm': 'sum:memory.memory.buffered.value'
            }
            r = requests.get(self.url, params=payload)
            resp = r.json()
            self.points = 21118 * self.amount

        elif self.engine == "kairosdb":
            self.remote = 'localhost'
            self.url = 'http://localhost:8080/api/v1/datapoints/query'
            payload = {
                'start_relative': {
                    'value': '5',
                    'unit': 'days'
                },
                'metrics': [
                    {
                        'name': 'memory.memory.buffered.value'
                    }
                ]
            }
            r = requests.post(self.url, data=json.dumps(payload))
            resp = r.json()
            self.points = resp['queries'][0]['sample_size'] * self.amount

    def aggregateTest(self):
        if self.engine == "influxdb":
            self.remote = 'localhost'
            self.url = 'http://localhost:8086/db/graphite/series'
            payload = {
                'u': 'root',
                'p': 'root',
                'q': 'select mean(value) from random_host_1.cpu-0.cpu-idle group by time(10m)'
            }
            r = requests.get(self.url, params=payload)
            resp = r.json()
            self.points = 705 * self.amount

        elif self.engine == "opentsdb":
            self.remote = 'localhost'
            self.url = 'http://localhost:4242/api/query'
            payload = {
                'start': 1404545541,
                'end': 1404755263,
                'm': 'avg:10m-avg:memory.memory.cached.value'
            }
            r = requests.get(self.url, params=payload)
            resp = r.json()
            self.points = 343 * self.amount

        elif self.engine == "kairosdb":
            self.remote = 'localhost'
            self.url = 'http://localhost:8080/api/v1/datapoints/query'
            payload = {
                "metrics": [
                    {
                        "name": "memory.memory.used.value",
                        "aggregators": [
                            {
                                "name": "sum",
                                "sampling": {
                                    "value": "10",
                                    "unit": "minutes"
                                }
                            }
                        ]
                    }
                ],
                "start_relative": {
                    "value": "5",
                    "unit": "days"
                }
            }
            r = requests.post(self.url, data=json.dumps(payload))
            #resp = r.json()
            self.points = 180 * self.amount

    def sendTest(self):
        for metric_name in metrics:
            if self.engine == "kairosdb":
                self.url = 'http://localhost:8080/api/v1/datapoints/'
                payload = [
                    {
                        'name': metric_name+".sendTest",
                        'datapoints': [
                            [int(random.randint(one_year_ago, now)) * 1000,
                                random.randint(0, 100)] for x in range(self.amount)
                        ],
                        'tags': {
                            'host': 'sendTestHost'
                        }
                    }
                ]
            elif self.engine == "influxdb":
                self.url = 'http://localhost:8086/db/test_graphite/series?u=root&p=root'
                payload = [
                    {
                        'name': metric_name+".sendTest",
                        'columns': ['time', 'value'],
                        'points': [
                            [int(random.randint(one_year_ago, now)) * 1000,
                                random.randint(0, 100)] for x in range(self.amount)
                        ]
                    }
                ]

            requests.post(self.url, data=json.dumps(payload))

    def run(self):
        # print "*" * 50
        # print "Requesting memory.memory.buffered.value %s times" % self.amount
        #
        # print "Points\t\tTime\t\tEngine\t\tPer Point"
        #
        # self.engine = "influxdb"
        # time = timeit.timeit(self.runTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tInfluxDB\t{:10.8f}".format(self.points, time, perpoint)
        #
        # self.engine = "opentsdb"
        # time = timeit.timeit(self.runTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tOpenTSDB\t{:10.8f}".format(self.points, time, perpoint)
        #
        # self.engine = "kairosdb"
        # time = timeit.timeit(self.runTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tKairosDB\t{:10.8f}".format(self.points, time, perpoint)
        #
        # print "*" * 50
        # print "Requesting memory.memory.buffered.value %s times grouped by 10m" % self.amount
        #
        # print "Points\t\tTime\t\tEngine\t\tPer Point"
        #
        # self.engine = "influxdb"
        # time = timeit.timeit(self.aggregateTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tInfluxDB\t{:10.8f}".format(self.points, time, perpoint)
        #
        # self.engine = "opentsdb"
        # time = timeit.timeit(self.aggregateTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tOpenTSDB\t{:10.8f}".format(self.points, time, perpoint)
        #
        # self.engine = "kairosdb"
        # time = timeit.timeit(self.aggregateTest, number=self.amount)
        # perpoint = time / self.points
        # print "{:10}\t{:10.4f}\tKairosDB\t{:10.8f}".format(self.points, time, perpoint)

        print "*" * 50
        print "Sending %s points" % self.amount

        print "Points\t\tTime\t\tEngine\t\tPer Point"

        self.engine = "influxdb"
        time = timeit.timeit(self.sendTest, number=1)
        perpoint = time / self.amount
        print "{:10}\t{:10.4f}\tInfluxDB\t{:10.8f}".format(self.amount, time, perpoint)

        self.engine = "kairosdb"
        time = timeit.timeit(self.sendTest, number=1)
        perpoint = time / self.amount
        print "{:10}\t{:10.4f}\tKairosDB\t{:10.8f}".format(self.amount, time, perpoint)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-amt", "--amount", help="how many metrics to request", type=int, required=True)
    args = parser.parse_args()

    mp = MetricGrabber(args.amount)
    mp.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
