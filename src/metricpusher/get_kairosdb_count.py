import requests
import json
import timeit
import time
import metrics

metrics = metrics.metrics

series = [
    "cpu-3.cpu-interrupt",
    "memory.memory.buffered.value",
    "vmem.vmpage_io-memory",
    "processes.ps_state.sleeping.value"
]


def getCount():
    """Repeatedly counts all the data points for KairosDB under the stressTest tag"""
    total_count = 0
    HOST = "localhost"
    PORT = 8080
    url = 'http://%s:%s/api/v1/datapoints/query' % (HOST, str(PORT))
    for metric_name in metrics:

        payload = {
            "metrics": [
                {
                    "tags":
                    {
                        "host": ["stressTest"]
                    },
                    "name": metric_name,
                    "aggregators": [
                        {
                            "name": "count",
                            "sampling": {
                                "value": "1",
                                "unit": "months"
                            }
                        }
                    ]
                }
            ],
            "cache_time": 0,
            "start_relative": {
                "value": "1",
                "unit": "years"
            }
        }
        r = requests.post(url, data=json.dumps(payload))
        resp = r.json()
        print resp
        sample_size = resp['queries'][0]['sample_size']
        total_count += sample_size

    print "Total number of data points: %s" % total_count

while True:
    print "*" * 70
    timer = timeit.timeit(getCount, number=1)
    print "Request at %s took %s seconds" % (time.ctime(), timer)
    time.sleep(360)
