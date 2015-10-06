# Running on AWS

## First

- Alias to the script to make the below clearer.

      export SPARK_EC2=/Users/joe/Downloads/spark-1.1.1/ec2/spark-ec2

- set the AWS key in your environment (do this each run)

      export AWS_ACCESS_KEY_ID=KEY_HERE
      export AWS_SECRET_ACCESS_KEY=KEY_HERE
      
## Start cluster

- Start a cluster with 5 slaves in `us-west-2`, which is in Oregon. Important. Replace key path if you need to (and in subsequent commands).

       $SPARK_EC2 -k laskuri-oregon -i ~/.ssh/laskuri-oregon.pem --copy-aws-credentials --instance-type=m1.xlarge -s 10 -r us-west-2 launch laskuri


This will take a few minutes longer than you think. Like 10 minutes longer.

- Log into the master node

      $SPARK_EC2 -k laskuri-oregon -i ~/.ssh/laskuri-oregon.pem -r us-west-2 login laskuri

## Get data in

  ~/ephemeral-hdfs/bin/hadoop fs -copyFromLocal /mnt2/RS_full_corpus /input/RS_full_corpus


## Get code in


      mkdir /root/spark/code
      cd /root/spark/code
      git clone https://github.com/CrossRef/reddit-dump-experiment.git
      cd reddit-dump-experiment
      sbt assembly

Remember the location of the jar, e.g. `/root/spark/code/reddit-dump-experiment/target/scala-2.10/reddit-dump-experiment-assembly-0.1-SNAPSHOT.jar`.

Deploy JAR

      ~/spark-ec2/copy-dir /root/spark/code


## Run

      time ~/spark/bin/spark-submit \
        --conf spark.reddit.inputfile="hdfs://input/RS_full_corpus.bz2" \
        --conf spark.reddit.outputdir="/output" \
        --conf spark.reddit.tasks="publisherYearDomainCountChart,publisherYearMonthDomainCountChart,publisherDomainList,doiList" \
        --class org.crossref.reddit.Main /root/spark/code/reddit-dump-experiment/target/scala-2.10/reddit-dump-experiment-assembly-0.1-SNAPSHOT.jar


## Get stuff out

    ~/ephemeral-hdfs/bin/hadoop fs -copyToLocal /output/publisher-domain-list /tmp/publisher-domain-list

    yum install aws-cli

    