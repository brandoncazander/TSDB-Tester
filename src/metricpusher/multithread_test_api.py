import random
import requests
import sys
import timeit
import json
import time
from multiprocessing import Process
import metrics

#Time Constants
NOW = 1404776380
ONE_YEAR_AGO = 1404776380 - 31557600


class StressTester(object):
    """Not sure what this does anymore"""
    def __init__(self, engine, processes):
        self.engine = engine
        self.processes = processes

        if self.engine == "influxdb":
            self.url = 'http://74.121.32.117:8086/db/test_graphite/series?u=root&p=root'
        elif self.engine == "kairosdb":
            self.url = 'http://74.121.32.116:8080/api/v1/datapoints/'

    def _setup(self):
        for num in range(0, self.processes):
            print "Starting process #%s" % self.processes
            p = Process(target=self._start)
            p.start()

    def _start(self):
        pass


class MultithreadTestAPI(object):
    def __init__(self):
        pass

    def _setup(self):
        """
        Pressure test
        """
        print "*" * 50
        print "Sending 61 metrics to InfluxDB"
        print "*" * 50

        # Setup
        self.url = 'http://74.121.32.117:8086/db/test_graphite/series?u=root&p=root'

        cur_test = 1
        self.amount = 5
        while cur_test < 50:
            cur_time = int(time.time()) * 1000
            self.amount = self.amount * 2
            print "Test: %s\tTime: %s\tAmount: %s" % (cur_test, cur_time, self.amount)
            self.engine = "influxdb"
            self.sender()
            self.engine = "kairosdb"
            cur_test += 1

    def sender(self):
        for metric_name in metrics:
            if self.engine == "kairosdb":
                self.url = 'http://74.121.32.116:8080/api/v1/datapoints/'
                self.payload = [
                    {
                        'name': metric_name+".0123456789",
                        'datapoints': [
                            [int(random.randint(ONE_YEAR_AGO, NOW)) * 1000,
                                random.randint(0, 100)] for x in range(self.amount)
                        ],
                        'tags': {
                            'host': 'sendTestHost'
                        }
                    }
                ]

            elif self.engine == "influxdb":
                self.url = 'http://74.121.32.117:8086/db/test_graphite/series?u=root&p=root'
                self.payload = [
                    {
                        'name': metric_name+".0123456789",
                        'columns': ['time', 'value'],
                        'points': [
                            [int(random.randint(ONE_YEAR_AGO, NOW)) * 1000,
                                random.randint(0, 100)] for x in range(self.amount)
                        ]
                    }
                ]

            requests.post(self.url, data=json.dumps(self.payload))

    def sendTest(self, amount):

        influx_time = 0
        kairos_time = 0

        for metric_name in metrics:
            self.url = 'http://74.121.32.116:8080/api/v1/datapoints/'
            self.payload = [
                {
                    'name': metric_name+".perfTest",
                    'datapoints': [
                        [int(random.randint(ONE_YEAR_AGO, NOW)) * 1000,
                            random.randint(0, 100)] for x in range(amount)
                    ],
                    'tags': {
                        'host': 'sendTestHost'
                    }
                }
            ]
            influx_time += timeit.timeit(self.sender, number=1)

            self.url = 'http://74.121.32.117:8086/db/test_graphite/series?u=root&p=root'
            self.payload = [
                {
                    'name': metric_name+".perfTest",
                    'columns': ['time', 'value'],
                    'points': [
                        [int(random.randint(ONE_YEAR_AGO, NOW)) * 1000,
                            random.randint(0, 100)] for x in range(amount)
                    ]
                }
            ]

            kairos_time += timeit.timeit(self.sender, number=1)

        print "{:10.4f}\tInfluxDB".format(influx_time)
        print "{:10.4f}\tKairosDB".format(kairos_time)


def main():
    test = MultithreadTestAPI()
    test._setup()

    return 0

if __name__ == "__main__":
    sys.exit(main())
