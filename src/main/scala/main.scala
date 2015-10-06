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
import scala.util.parsing.json._
import java.util.Date
import java.text.SimpleDateFormat 
import java.net.{HttpURLConnection, URL, URLDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io._ 
import java.io.BufferedOutputStream
import org.apache.hadoop.conf.Configuration

// Search for domains that belong to members of Crossref, i.e. that domains resolve to. This list is tored in src/main/resources/publisherdomains.json
// See README.md for instructions about re-generating this.
object MemberDomains {
  val input = scala.io.Source.fromInputStream(Main.getClass.getClassLoader().getResourceAsStream("publisherdomains.json")).mkString
  val json: Option[Any] = JSON.parseFull(input)
  
  // Sequence of (substring, list-of-domains), in order.
  // It would be nice to specify this as List[Tuple2[String, List[String]]] but doesn't look like that can happen automatically.
  // Instead we have to settle for Any.
  val structure: List[List[Any]] = json.get.asInstanceOf[List[List[Any]]]

  def matches(soughtDomain: String) : Option[String] = {
    // Linear search of greatest common substrings in order of popularity. These are mutually exclusive, so find only the first match.
    val substringMatch = structure.find{case List(substring: String, domains: List[String]) => 
      soughtDomain.contains(substring)
    }

    // If found, linear search for domains that have the substring as the greatest common factor.
    substringMatch match {
      case Some(List(_, domains: List[String])) => domains.find{domain => soughtDomain.contains(domain)}
      case None => None
    }
  }
}

object Main {
  val memberDomains = MemberDomains

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
    allText: String,
    original: String)

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
        allText,
        line)

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
    try {
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
    } catch {
      // All manner of sins can happen resolving a URL. They all count for nothing.
      case _ : Throwable => None
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
     // ! memberDomains.find(domain => line.indexOf(domain) != -1).isEmpty
     ! memberDomains.matches(line).isEmpty
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

  def writeStringBuilderToFile(input: StringBuilder, filename: String, config : Configuration) {
    val fs = FileSystem.get(config); 
    val bos = new BufferedOutputStream(fs.create(new Path(filename)))
    bos.write(input.toString().getBytes("UTF-8"))
    bos.close()
  }

  // Aggregate
  def count(lines: RDD[_]) = {
    lines.map(x => (x, 1)).reduceByKey(_ + _)
  }

  // Publisher domains year count
  def publisherYearDomainCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()

    count(lines.map(line => line.created_year))
      .collect()
      .map{case (year, count) => "%s\t%d\n".format(year, count)}
      .sorted
      .foreach(x => sb.append(x))

    val fs = FileSystem.get(lines.sparkContext.hadoopConfiguration); 
    val output = fs.create(new Path(outputDir + "/chart-publisher-domain-count"));
    output.writeUTF(sb.toString())
    output.close()
  }

  // Publisher year month count
  def publisherYearMonthDomainCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()


    count(lines.map(line => line.created_year_month))
      .collect()
      .map{case (yearMonth, count) => "%s\t%d\n".format(yearMonth, count)}
      .sorted
      .foreach(x => sb.append(x))

    writeStringBuilderToFile(sb, outputDir + "/chart-publisher-domain-count", lines.sparkContext.hadoopConfiguration)
  }

  // Year count
  def yearCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()

    
    count(lines.map(line => line.created_year))
      .collect()
      .map{case (year, count) => "%s\t%d\n".format(year, count)}
      .sorted
      .foreach(x => sb.append(x))
    
    writeStringBuilderToFile(sb, outputDir + "/chart-year-count", lines.sparkContext.hadoopConfiguration)
  }

  // Year month count
  def yearMonthCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()

    
    count(lines.map(line => line.created_year_month))
      .collect()
      .map{case (yearMonth, count) => "%s\t%d\n".format(yearMonth, count)}
      .sorted
      .foreach(x => sb.append(x))

    writeStringBuilderToFile(sb, outputDir + "/chart-year-month-count", lines.sparkContext.hadoopConfiguration)
  }

  def votesMonthCount(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()


    val yearMonthCounts = lines.map(line => (line.created_year_month, (line.ups, -line.downs, line.ups - line.downs)))
      .reduceByKey{case Tuple2((u1, d1, s1), (u2, d2, s2)) => (u1 + u2, d1 + d2, s1 + s2)}
      .collect()

    sb.append("year\tupvote\tdownvote\tscore\n")
    yearMonthCounts.sortBy{case Tuple2(line, _ ) => line}.foreach{case (year, (up, down, score)) => {
      sb.append("%s\t%d\t%d\t%s\n".format(year, up, down, score))
    }}

    writeStringBuilderToFile(sb, outputDir + "/chart-year-month-votes", lines.sparkContext.hadoopConfiguration)
  }

    // Year subreddit count
  def yearSubredditCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()
    
    val cysrData = count(lines.map(line => Tuple2(line.created_year, line.subreddit))).collect()

    val subreddits = cysrData.map{ case Tuple2(Tuple2(_, subreddit: String), _) => subreddit}.distinct.sorted
    val years = cysrData.map{case Tuple2(Tuple2(year : String, _), _) => year}.distinct.sorted

    sb.append("year\t" + subreddits.mkString("\t") + "\n")
    years.foreach(year => {
      sb.append(year.toString)
      subreddits.foreach(subreddit => {
        // Linear search but there isn't much data here.
        sb.append(cysrData.filter{case Tuple2(x, _) => x == Tuple2(year, subreddit)}.headOption match {
          case Some(Tuple2(_, count)) => "\t" + count.toString
          case None => "\t" + "0"})
        })
      sb.append("\n")
      })

    writeStringBuilderToFile(sb, outputDir + "/chart-year-subreddit-count", lines.sparkContext.hadoopConfiguration)
  }

  def yearMonthSubredditCountChart(lines: RDD[Line], outputDir : String) {
    val sb = new StringBuilder()

     // Year month subreddit count
    val cymsrData = count(lines.map(line => Tuple2(line.created_year_month, line.subreddit))).collect()

    val subreddits = cymsrData.map{ case Tuple2(Tuple2(_, subreddit : String), _) => subreddit}.distinct.sorted
    val years = cymsrData.map{case Tuple2(Tuple2(yearMonth : String, _), _) => yearMonth}.distinct.sorted

    sb.append("yearMonth\t")
    sb.append(subreddits.mkString("\t") + "\n")
    years.foreach(yearMonth => {
      sb.append(yearMonth.toString())
      subreddits.foreach(subreddit => {
        // Linear search but there isn't much data here.
        sb.append(cymsrData.filter{case Tuple2(x, _) => x == Tuple2(yearMonth, subreddit)}.headOption match {
          case Some(Tuple2(_, count)) => "\t" + count.toString
          case None => "\t" + "0"})
        })
      sb.append("\n")
      })

    writeStringBuilderToFile(sb, outputDir + "/chart-year-month-subreddit-count", lines.sparkContext.hadoopConfiguration)
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

      result
    }
  }

  def urlFromLine(line: Line) : Seq[String] = {
    line.url match {
      case "" => List()
      case url => List(url)
    }
  }

  def doiList(lines: RDD[Line], outputDir : String) {
    val dois = lines.flatMap(doiFromLine).repartition(1)

    dois.saveAsTextFile(outputDir + "/doi-list") 
  }

  def doiListEntire(lines: RDD[Line], outputDir : String) {
    // Input already filtered to contain only DOI lines, nothing more to do.
    val dois = lines.map(_.original).repartition(1)
    dois.saveAsTextFile(outputDir + "/doi-list-entire") 
  }

  def publisherDomainList(lines: RDD[Line], outputDir : String) {
    // For now we're only going to get the domains, even though we searched in the full text for the publisher domain.
    // This means that this list may be shorter than that reported by the count.
    val dois = lines.flatMap(urlFromLine).distinct.repartition(1)

    dois.saveAsTextFile(outputDir + "/publisher-domain-list") 
  }

  // Save entire line from log.
  def publisherDomainListEntire(lines: RDD[Line], outputDir : String) {
    // Input already filtered to contain only publisher domains, nothing more to do.
    lines.map(_.original).saveAsTextFile(outputDir + "/publisher-domain-list-entire") 
  }

  def main(args: Array[String]) {      
    val sparkConf = new SparkConf()
    val sc =  new SparkContext(sparkConf)    
    
    val inputFile = sparkConf.get("spark.reddit.inputfile")
    val outputDir = sparkConf.get("spark.reddit.outputdir")
    val tasks = sparkConf.get("spark.reddit.tasks")

    // As these are lazily created, if there are no tasks that use them, no problem.
    // Lines of DOIs.
    val doiInput = sc.textFile(inputFile).filter(likelyDOI).flatMap(parse).filter(hasDOI).persist(StorageLevel.DISK_ONLY)

    // Lines of publisher domains that could be DOIs.
    val publisherDomainInput = sc.textFile(inputFile).filter(likelyPublisherDomain).flatMap(parse).persist(StorageLevel.DISK_ONLY)

    if (tasks.contains("yearCountChart")) {
      yearCountChart(doiInput, outputDir)
    }

    if (tasks.contains("yearMonthCountChart")) {
      yearMonthCountChart(doiInput, outputDir)
    }

    if (tasks.contains("yearSubredditCountChart")) {
      yearSubredditCountChart(doiInput, outputDir)
    }

    if (tasks.contains("yearMonthSubredditCountChart")) {
      yearMonthSubredditCountChart(doiInput, outputDir)
    }

    if (tasks.contains("votesMonthCount")) {
      votesMonthCount(doiInput, outputDir)
    }

    if (tasks.contains("publisherYearDomainCountChart")) {
      publisherYearDomainCountChart(publisherDomainInput, outputDir)
    }

    if (tasks.contains("publisherYearMonthDomainCountChart")) {
      publisherYearMonthDomainCountChart(publisherDomainInput, outputDir)
    }

    if (tasks.contains("publisherDomainList")) {
      publisherDomainList(publisherDomainInput, outputDir)
    }

    if (tasks.contains("doiList")) {
      doiList(doiInput, outputDir)
    }

    if (tasks.contains("publisherDomainListEntire")) {
      publisherDomainListEntire(publisherDomainInput, outputDir)
    }

    if (tasks.contains("doiListEntire")) {
      doiListEntire(doiInput, outputDir)
    }

  }
}