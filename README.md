# Reddit dump experiment

Quick analysis to look at the use of DOIs in Reddit submissions over time. Can be run locally using these instructions or on a cluster using instructions found in Spark docs.

Code here is a little hacky, but does the job. Suggestions welcome.

## To use

Assuming you have Scala and SBT installed.

1. Grab a copy of the data from `https://www.reddit.com/r/datasets/comments/3mg812/full_reddit_submission_corpus_now_available_2006/`
2. Get a cup of tea.
3. `bunzip2 -df -v RS_full_corpus.bz2`
4. Get a cup of coffee.
5. Get a copy of Apache Spark from `https://spark.apache.org/`. Get a pre-built one.
6. Install `gnuplot`.

## To run

1. Compile to uberjar: `sbt assembly`.
2. `mkdir gnuplot/data`
3. Run with Spark runner (which will be wherever you downloaded it). Substitute the input file location.

		time ~/Downloads/spark-1.4.1-bin-hadoop2.6/bin/spark-submit \
		    --conf spark.reddit.inputfile="file:///tmp/RS_full_corpus-5gb" \
		    --conf spark.reddit.outputdir="./gnuplot/data" \
		    --master local[*] \
		    --class org.crossref.reddit.Main ./target/scala-2.10/reddit-dump-experiment-assembly-0.1-SNAPSHOT.jar

3. Data will go into gnuplot/data
4. Produce charts with `./plots.sh` . Plots will go into `gnuplot/output`.

## To tweak

You should probably take the first 5GB of the dump to get any data at all (assuming it starts in 2006). 

To update regular expression of publisher domains from the [member-domains](https://github.com/CrossRef/member-domains) project:

    lein run dump-domains > /path/to/reddit-dump-experiment/src/main/resources/publisherdomains.txt