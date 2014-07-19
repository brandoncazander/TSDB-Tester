import requests
import timeit
import time
import metrics

HOST = "localhost"
PORT = 8086

metrics = metrics.metrics
series = [
    "stressTest.cpu-3.cpu-interrupt",
    "stressTest.memory.memory.buffered.value",
    "stressTest.vmem.vmpage_io-memory",
    "stressTest.processes.ps_state.sleeping.value"
]


def getCount():
    """Repeatedly gets the count of data points in InfluxDB"""
    total_count = 0
    for series_name in series:
        url = 'http://%s:%s/db/graphite/series' % (HOST, str(PORT))
        payload = {
            'u': 'root',
            'p': 'root',
            'q': 'select * from ' + series_name
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
