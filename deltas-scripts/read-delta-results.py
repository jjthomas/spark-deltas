import os
import sys

# argv[1] is directory with output files produced by delta-submit.py, argv[2] is a
# filter for the test we are interested in
# outputs the percentage improvement from using deltas for each (# groups, # deltas)
names = os.listdir(sys.argv[1])
for n in names:
    if n.startswith("out") and sys.argv[2] in n:
        times = []
        for l in open(os.path.join(sys.argv[1], n)):
            if "time:" in l:
                times.append(float(l.split(" ")[-1]))
        print "{0}: {1} {2}".format(n, round((1 - times[3]/times[2])*100), times)
