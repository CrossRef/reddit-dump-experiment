# Reddit dump experiment

Quick analysis to look at the use of DOIs in Reddit submissions over time. Can be run locally using these instructions or on a cluster using instructions found in Spark docs.

Code here is a little hacky, but does the job. Suggestions welcome.

Outputs:

 - `yearCountChart` - DOI mentions per year in gnuplot format
 - `yearMonthCountChart` - DOI mentions per month in gnuplot format
 - `yearSubredditCountChart` - Subreddits that mention DOIs, per year, gnuplot format
 - `yearMonthSubredditCountChart` - Subreddits that mention DOIs, per month, gnuplot format
 - `votesMonthCount` - Up/down votes in posts that mention DOIs, per month, gnuplot format
 - `publisherYearDomainCountChart` - Count of mentions of URLs that could be DOIs, per year, gnuplot format
 - `publisherYearMonthDomainCountChart` - Count of mentions of URls that could be DOIs, per month, gnuplot format
 - `publisherDomainList` - List of distinct URLs that belong to domains that could be DOIs. List of text.
 - `doiList` - List of distinct DOIs that belong to domains that could be DOIs. List of text.


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
		    --conf spark.reddit.outputdir="./output" \
        
        --conf spark.reddit.tasks="publisherYearDomainCountChart,publisherYearMonthDomainCountChart,publisherDomainList,doiList"
		    --master local[*] \
		    --class org.crossref.reddit.Main ./target/scala-2.10/reddit-dump-experiment-assembly-0.1-SNAPSHOT.jar

    # full complement:
    --conf spark.reddit.tasks="yearCountChart,yearMonthCountChart,yearSubredditCountChart,yearMonthSubredditCountChart,votesMonthCount,publisherYearDomainCountChart,publisherYearMonthDomainCountChart,publisherDomainList,doiList"

3. Data will go into gnuplot/data
4. Produce charts with `./plots.sh` . Plots will go into `gnuplot/output`.

## To tweak

You should probably take the first 5GB of the dump to get any data at all (assuming it starts in 2006). 

To update regular expression of publisher domains from the [member-domains](https://github.com/CrossRef/member-domains) project. NB you'll need to repackage the JAR with `sbt assembly`.

    time lein run dump-common-substrings > ~/sc/reddit-dump-experiment/src/main/resources/publisherdomains.json


## Member domains

There is a list of member domains 

Benchmarks on Joe's Macbook Air, master local[*]

### Linear vs greatest common factor substrings

publisherYearDomainCountChart only, 1.5 GB of Reddit

Linear search of domains: 10 minutes
Using substrings, word-count-threshold 5: 27 seconds

### Between different word-count-thresholds:

publisherYearDomainCountChart only, 5.6 GB of Reddit

5: 1m47.939s
10: 1m46.234s
20: 1m43.847s

Tweaking it doesn't seem to make much odds.

### Extract full post line

The `likelyPublisherDomain` filter (e.g. `publisherYearDomainCountChart` and `publisherYearMonthDomainCountChart`) searches whole post but `publisherDomainList` only returns the URLs found in submissions. Some links are found in `selftext`, `description` or `media`, these are not returned.

Example run:

return whole line 7m10.045s, 9962 lines
return just domain 7m15.436s, 9128 lines

So the numbers are in the right ball-park. It's probably worth saving the expense of extracting and verifying URLs at this volume.