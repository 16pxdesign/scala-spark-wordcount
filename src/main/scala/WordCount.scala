package spark.wordcount

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("word count")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://master:9000/books/*.txt")
    val words = data.flatMap("""[a-zA-ZÀ-ÿ`'-]+""".r findAllIn _)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
    val regex = """[^/]+$""".r
    val langs = sc.wholeTextFiles("hdfs://master:9000/langs/*")
      .flatMap(line => (line._2.split(" ").map(word => (word, regex.findFirstIn(line._1).mkString))))
    val out = words.take(50)
      .map(line => (line._1, langs.filter(x => x._1.equalsIgnoreCase(line._1)).map(y => y._2).collect.mkString, line._2))
    //(word,lang,count)
    out.foreach(l => println(l))
    sc.parallelize(out).saveAsTextFile("hdfs://master:9000/myout")


  }
}

