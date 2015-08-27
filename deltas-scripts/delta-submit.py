# executes these the below two test jobs with varying numbers of groups and deltas and writes
# to the output files specified in cmd
import os

progs = ["ComputeOnlyDeltaTest", "ComputeOnlyDeltaTwoStageTest"]
groups = [100, 1000, 10000, 100000, 1000000]
deltas = [5, 500, 5000, 50000, 500000, 5000000]


cmd = ("C:\\hadoop\\spark-1.2.1.2.2.4.2-0002\\bin\\spark-submit --verbose --class com.microsoft.dsoap.deltatests.{0} "
       "--master spark://XXX:7077 "
       "--num-executors 5 --executor-memory 20g --executor-cores 1 D:\\spark-hdfs-loader-1.0-SNAPSHOT.jar "
       "10000000 {1} {2} {2} 5 20 > D:\\delta-tests\\out-{0}-{1}-{2} 2>D:\\delta-tests\\err-{0}-{1}-{2}")

for p in progs:
    for g in groups:
        for d in deltas:
            run = cmd.format(p, g, d)
            print run
            os.system(run)