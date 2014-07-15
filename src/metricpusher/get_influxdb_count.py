import requests
import json
import timeit
import time
import metrics


metrics = metrics.metrics
series = [
          "entropy.entropyhttp_api_test",
          "netlink-eth2.if_octets.txhttp_api_test",
          "cpu-6.cpu-userhttp_api_test",
          "random_host.cpu-2.cpu-steal",
          "entropy.entropytestetsetsetsetsetsetset",
          "random.cpu-2.cpu-steal",
          "vmem.vmpage_number-file_pageshttp_api_test",
          "rand.cpu-2.cpu-steal",
          "netlink-eth2.if_octets.rxhttp_api_test",
        ]

"""
Repeatedly gets the count of data points in InfluxDB
"""
def getCount():
    total_count = 0
    for series_name in series:
        series_count = 0
        url = 'http://74.121.32.117:8086/db/graphite/series'
        payload = {
            'u': 'root',
            'p': 'root',
            'q': 'select count(value) from '+ series_name
        }
        r = requests.get(url, params=payload)
        resp = r.json()
        for num in range(0, len(resp)):
            print "%s: %s\t\t\t%s" % (num, resp[num]['name'], resp[num]['points'][0][1])
            total_count += int(resp[num]['points'][0][1])

    print "Total number of data points: %s" % total_count

while True:
    print "*" * 70
    timer = timeit.timeit(getCount, number=1)
    print "Request at %s took %s seconds" % (time.ctime(), timer)
    time.sleep(360)