# skala-spark-wordcount

```java
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

```

![image](https://user-images.githubusercontent.com/28375942/136126583-1c50f876-7ff3-483c-b78e-c7cea7ece004.png)
![image](https://user-images.githubusercontent.com/28375942/136126589-6a2c27f6-abfa-4e93-b5e1-4f13c982bd27.png)
![image](https://user-images.githubusercontent.com/28375942/136126605-6973756f-aa18-4fc4-9e4d-38a7d01d038a.png)
![image](https://user-images.githubusercontent.com/28375942/136126620-f87f2e34-f3c3-4fdd-ac71-a0f6d8080a38.png)
![image](https://user-images.githubusercontent.com/28375942/136126633-d7d44fe4-7050-4779-9122-23da47545ca4.png)
![image](https://user-images.githubusercontent.com/28375942/136126639-e16fec52-ec59-46a7-81a3-6b3a36256b40.png)

