"""
Converts CollectD CSV files into one CSV file for use with metric_pusher.py
"""
import os

w = open('metric_0.csv', 'w')

for dirname, dirnames, filenames in os.walk('./data'):
    for filename in filenames:
        title = dirname.split("/")
        print "Working on %s" % title
        metric_name = "%s.%s" % (title[3], filename[:-11])

        f = open(os.path.join(dirname, filename), 'r')
        headers = f.readline().rstrip("\n").split(",")
        if len(headers) == 2:
            for cur_line in f:
                cur_line = cur_line.split(",")
                message = metric_name + ", %s, %s, %s\n" % (cur_line[0], cur_line[1].rstrip('\n'), title[2])
                w.write(message)
            f.close()
        elif len(headers) == 3:
            for cur_line in f:
                cur_line = cur_line.split(",")
                message = metric_name + ".%s, %s, %s, %s\n" % (headers[1], cur_line[0], cur_line[1].rstrip('\n'), title[2])
                w.write(message)
                message = metric_name + ".%s, %s, %s, %s\n" % (headers[2], cur_line[0], cur_line[2].rstrip('\n'), title[2])
                w.write(message)
            f.close()



w.close()