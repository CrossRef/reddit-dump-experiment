package org.crossref.reddit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD}
import net.liftweb.json._
import net.liftweb.json.Serialization.{read}
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import math.max 
import java.io.PrintWriter
import scala.util.parsing.json._
import java.util.Date
import java.text.SimpleDateFormat 
import java.net.{HttpURLConnection, URL, URLDecoder}

object Main {
  val memberDomainRegularExpression = scala.io.Source.fromInputStream(Main.getClass.getClassLoader().getResourceAsStream("publisherdomains.txt")).getLines().next().r
  val memberDomains = scala.io.Source.fromInputStream(Main.getClass.getClassLoader().getResourceAsStream("publisherdomains.txt")).getLines().toArray

  // Input
  case class Line(
    id: String,
    ups: Integer,
    downs: Integer,
    score: Integer,
    url: String,
    created_utc: Date,
    created_year: String,
    created_year_month: String,
    domain: String,
    subreddit: String,
    selfText: String,
    description: String,
    allText: String)

  // Extract all text values from a JSON map recursively.
  def stuffFromMap (input: Map[String, Any]) : Iterable[String] = {
    input.flatMap{
      case Tuple2(a : String, b : Any) => {
        b match {
          case value : String => List(value)
          case anotherMap : Map[String, Any] => stuffFromMap(anotherMap.asInstanceOf[Map[String, Any]])
          case default => List()}
        }
      // e.g. null values.
      case default => List()
    }
  }

  def parse (line: String) : Seq[Line] = {
    try {

      val json: Option[Any] = JSON.parseFull(line)
      val map: Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]

      // Just everything stringy. Last resort.
      val allText = stuffFromMap(map).mkString(" ")

       // This is coming from JSON. No integers.
      val ups = map.get("ups").get.asInstanceOf[Double].toInt
      val downs = map.get("downs").get.asInstanceOf[Double].toInt

      val date = new java.util.Date(map.get("created_utc").get.asInstanceOf[String].toLong * 1000)

      // SimpleDateFormat isn't threadsafe.
      val yyyyMM = new SimpleDateFormat("yyyy-MM")
      val yyyy = new SimpleDateFormat("yyyy")

      val result = new Line(
        map.get("id").get.asInstanceOf[String],
        ups,
        downs,
        ups - downs,
        map.get("url").get.asInstanceOf[String],
        date,
        yyyy.format(date),
        yyyyMM.format(date),
        map.get("domain").get.asInstanceOf[String],
        map.get("subreddit").get.asInstanceOf[String],
        map.get("selftext").getOrElse("").asInstanceOf[String],
        map.get("description").getOrElse("").asInstanceOf[String],
        allText)

      List(result)
    } catch {
      // Input may be mysteriously malformed.
      case e : java.util.NoSuchElementException => {
        println("ERROR " + line)
        List()}
    }
  }

  def doiRe = "(?i)10.\\d{4,9}/[-._;()/:A-Z0-9]+".r

  // Extract the first DOI we find. Knock characters off the end until we get one that maches.
  def extractDOI(input: String) : Option[String] = {
      // https://www.reddit.com/wiki/commenting
      val prepared = input.replace("\\)", ")").replace("\\(", "(")
      val unencoded = URLDecoder.decode(prepared,"UTF-8")

      doiRe.findFirstIn(unencoded) match {
        case None => None
        case Some(matchedLink) => {
          // find returns an option
          val firstLink = (0 to 4)
            .map(n => matchedLink.dropRight(n))
            .find(doi => {
              val url = "http://doi.org/" + doi
              val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]

              connection.setInstanceFollowRedirects(false)

              val responseCode = connection.getResponseCode()

              (responseCode / 100 == 3) || (responseCode / 100 == 2) })
              
          firstLink
        }
      }
  }

  // Filter lines that probably contain a DOI to avoid parsing them.
  def likelyDOI (line : String) : Boolean = {
    val result = line.contains("doi.org/10.")

    // Only enable this if you are debugging things you know to contain DOIs.
    // if (!result) {
    //   println("LIKELY NOT " + line)
    // }

    result
  }

  def likelyPublisherDomain (line: String) : Boolean = {
     ! memberDomains.find(domain => line.indexOf(domain) != -1).isEmpty
  }

  def hasDOI (line : Line) : Boolean = {
    // Quick things first.
    val hasDoi = line.domain == "dx.doi.org" || 
      line.domain == "doi.org" || 
      line.domain.contains("doi.org/10.") || 
      line.allText.contains("doi.org/10.") 

    // Only enable this if you are debugging things you know to contain DOIs.
    // if (!hasDoi) {
    //   println("HAS NOT " + line)
    // }

    hasDoi
  }

  // Aggregate
  def count(lines: RDD[_]) = {
    lines.map(x => (x, 1)).reduceByKey(_ + _)
  }

  // Publisher domains year count
  def publisherYearDomainCountChart(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-publisher-domain-count")
    count(lines.map(line => line.created_year))
      .collect()
      .map{case (year, count) => "%s\t%d".format(year, count)}
      .sorted
      .foreach(output.println)
    output.close()
  }

  // Publisher year month count
  def publisherYearMonthDomainCountChart(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-publisher-domain-count")
    count(lines.map(line => line.created_year_month))
      .collect()
      .map{case (yearMonth, count) => "%s\t%d".format(yearMonth, count)}
      .sorted
      .foreach(output.println)
    output.close()
  }

  // Year count
  def yearCountChart(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-year-count")
    count(lines.map(line => line.created_year))
      .collect()
      .map{case (year, count) => "%s\t%d".format(year, count)}
      .sorted
      .foreach(output.println)
    output.close()
  }

  // Year month count
  def yearMonthCountChart(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-year-month-count")
    count(lines.map(line => line.created_year_month))
      .collect()
      .map{case (yearMonth, count) => "%s\t%d"
      .format(yearMonth, count)}
      .sorted
      .foreach(output.println)
    output.close()
  }

  def votesMonthCount(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-year-month-votes")

    val yearMonthCounts = lines.map(line => (line.created_year_month, (line.ups, -line.downs, line.ups - line.downs)))
      .reduceByKey{case Tuple2((u1, d1, s1), (u2, d2, s2)) => (u1 + u2, d1 + d2, s1 + s2)}
      .collect()

    output.println("year\tupvote\tdownvote\tscore")
    yearMonthCounts.sortBy{case Tuple2(line, _ ) => line}.foreach{case (year, (up, down, score)) => {
      output.println("%s\t%d\t%d\t%s".format(year, up, down, score))
    }}

    output.close()
  }

  // Year subreddit count
  def yearSubredditCountChart(lines: RDD[Line], outputDir : String) {
    val output = new PrintWriter(outputDir + "/chart-year-subreddit-count")
    val cysrData = count(lines.map(line => Tuple2(line.created_year, line.subreddit))).collect()

    val subreddits = cysrData.map{ case Tuple2(Tuple2(_, subreddit: String), _) => subreddit}.distinct.sorted
    val years = cysrData.map{case Tuple2(Tuple2(year : String, _), _) => year}.distinct.sorted

    output.print("year\t")
    output.println(subreddits.mkString("\t"))
    years.foreach(year => {
      output.print(year)
      subreddits.foreach(subreddit => {
        output.print("\t")
        // Linear search but there isn't much data here.
        output.print(cysrData.filter{case Tuple2(x, _) => x == Tuple2(year, subreddit)}.headOption match {
          case Some(Tuple2(_, count)) => count
          case None => 0})
        })
      output.println()
      })
    output.close()
  }

  def yearMonthSubredditCountChart(lines: RDD[Line], outputDir : String) {
     // Year month subreddit count
    val output = new PrintWriter(outputDir + "/chart-year-month-subreddit-count")
    val cymsrData = count(lines.map(line => Tuple2(line.created_year_month, line.subreddit))).collect()

    val subreddits = cymsrData.map{ case Tuple2(Tuple2(_, subreddit : String), _) => subreddit}.distinct.sorted
    val years = cymsrData.map{case Tuple2(Tuple2(yearMonth : String, _), _) => yearMonth}.distinct.sorted

    output.print("yearMonth\t")
    output.println(subreddits.mkString("\t"))
    years.foreach(yearMonth => {
      output.print(yearMonth)
      subreddits.foreach(subreddit => {
        output.print("\t")
        // Linear search but there isn't much data here.
        output.print(cymsrData.filter{case Tuple2(x, _) => x == Tuple2(yearMonth, subreddit)}.headOption match {
          case Some(Tuple2(_, count)) => count
          case None => 0})
        })
      output.println()
      })
    output.close()
  }

  // Return a Seq pretending to be an Option, makes for more efficient filtering in Spark with flatMap.
  def doiFromLine(line : Line) : Seq[String] = {

    if (line.domain.indexOf("doi.org") != -1) {
      // First try the domain.
      val withoutResolver = line.url.replaceAll("^.+/10\\.", "10.")
      
      println("ID " + line.id + " DOI WR " + withoutResolver)
      List(withoutResolver)
    } else {
      // Failing that search the text.
      val extracted = extractDOI(line.allText)

      val result = extracted match {
        case None => List()
        case Some(doi) => List(doi)
      }

       if (result.isEmpty) {
          println("COULDN'T FIND " + line.id)
        }

      // extracted
      result
    }
  }

  def doiList(lines: RDD[Line], outputDir : String) {
    val dois = lines.flatMap(doiFromLine).repartition(1)

    dois.saveAsTextFile(outputDir + "/doi-list") 
  }

  def main(args: Array[String]) {      
    val sparkConf = new SparkConf()
    val sc =  new SparkContext(sparkConf)    
    
    val inputFile = sparkConf.get("spark.reddit.inputfile")
    val outputDir = sparkConf.get("spark.reddit.outputdir")
    
    // Lines of DOIs.
    val doiInput = sc.textFile(inputFile).filter(likelyDOI).flatMap(parse).filter(hasDOI).persist(StorageLevel.DISK_ONLY)

    // Lines of publisher domains that could be DOIs.
    val publisherDomainInput = sc.textFile(inputFile).filter(likelyPublisherDomain).flatMap(parse).persist(StorageLevel.DISK_ONLY)
    
    yearCountChart(doiInput, outputDir)
    yearMonthCountChart(doiInput, outputDir)
    yearSubredditCountChart(doiInput, outputDir)
    yearMonthSubredditCountChart(doiInput, outputDir)
    votesMonthCount(doiInput, outputDir)

    publisherYearDomainCountChart(publisherDomainInput, outputDir)
    publisherYearMonthDomainCountChart(publisherDomainInput, outputDir)

    doiList(doiInput, outputDir)
  }
}