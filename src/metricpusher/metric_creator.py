import random
import argparse
import sys
import metrics

metrics = metrics.metrics

#Defaults for metrics to cover one year
now = 1404776380
one_year_ago = 1404776380 - 31557600


def makeFakeMetrics(hosts, start_timestamp=one_year_ago, end_timestamp=now, resolution=10):
    """Generates fake random metrics using real metric names and stores in
    data/metric_#.csv files for use with metric_pusher.py
    """
    tag = "random_host_"

    random.seed()

    """
    Open as many files as specified
    """
    files = {}
    for host in range(0, hosts):
        # Open the file
        files[host] = (open('data/metric_%s.csv' % host, 'w'))

        # Make host name
        hostname = tag + str(host)
        print "Writing for host %s" % hostname

        # Make metrics
        for metric in metrics:

            print "Writing metric %s" % metric

            # Create range of data
            for point in range(start_timestamp, end_timestamp, resolution):
                amount = random.randint(0, 2338960000)
                message = "%s, %s, %s, %s\n" % (metric, point, amount, hostname)
                files[host].write(message)

        # Finished this file
        files[host].close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--hosts", help="number of hosts to make fake metrics for", type=int, required=True)
    parser.add_argument("-s", "--start_timestamp", help="starting unix timestamp", type=int, required=True)
    parser.add_argument("-e", "--end_timestamp", help="ending unix timestamp", type=int, required=True)
    parser.add_argument("-r", "--resolution", help="resolution in seconds", type=int, required=True)
    args = parser.parse_args()

    makeFakeMetrics(args.hosts, args.start_timestamp, args.end_timestamp, args.resolution)

    return 0


if __name__ == "__main__":

    sys.exit(main())
