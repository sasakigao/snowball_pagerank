import scala.util.Either
import scala.util.control.Exception

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}

import java.io._

/**
 * Based on the social relationship, iterates PageRank 10 times 
 * and output the top 20 ranks after each iteration.
 */
object SocialPageRank {
	val appName = "Page Rank"
       val followerHdfsPath = "hdfs:///wxtotal/snowball/social/follower/"
      	val memberHdfsPath = "hdfs:///wxtotal/snowball/social/member/"
	val ranksOutputPath = "hdfs:///wxtotal/snowball/social/"
	
	def main(args: Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	
  		val sc = new SparkContext(confSpark)

  		val rawFollowerRDD = sc.textFile(followerHdfsPath).map(safe(parse(_, "follow")))
  		val rawMemberRDD = sc.textFile(memberHdfsPath).map(safe(parse(_, "member")))

  		val followedListRDDRaw = rawFollowerRDD ++ rawMemberRDD
  		val followedListRDDBad = followedListRDDRaw.filter(_.isLeft).map(_.left.get)      // bad parse
  		followedListRDDBad.collect.foreach(println)

		val followedListRDD = followedListRDDRaw.filter(_.isRight).map(_.right.get)         // good parse
			.groupByKey
  			.mapValues(_.toList)
  			.partitionBy(new HashPartitioner(100))
                  	.persist()
  		var ranksRDD = followedListRDD.mapValues(_ => 1.0)

  		val iteTimes =  1 to 100
  		iteTimes.foreach{ round =>
  			val ranksNewRound = followedListRDD.join(ranksRDD)
  				.flatMap{ case (follower, (followedList, followerRank)) =>
  					followedList.map(followed => (followed, followerRank / followedList.size))}
  				.reduceByKey(_ + _)
  			ranksRDD = ranksNewRound.mapValues(0.15 + 0.85 * _)
  			val ranks = ranksRDD.collect.sortBy(x => -x._2).toList            // collect then sorted by rank
  			sc.parallelize(ranks, 1).saveAsTextFile(ranksOutputPath + "round" + round)
  			// ranksPrint(ranks, ranksOutputPath + "round" + round)

  			/*val ranks =ranksRDD.map(_.swap).sortByKey(false, 1)       // sort in RDD
  			ranks.saveAsTextFile(ranksOutputPath + "round" + round)*/
  		}

    		sc.stop()
  	}

  	// def ranksPrint(topRanks : Array[(String, Double)], outputPath : String) = {
  	// 	val printer = new PrintWriter(outputPath)
  	// 	topRanks.foreach{ case (follower, rank) =>
  	// 		printer.println(s"${follower} ---- ${rank}")
  	// 	}
  	// 	printer.close
  	// }

  	def parse(line : String, mode : String) = {
  		val fields = line.split(",")
  		mode match {
  			case "follow" => (fields(1), fields(0))
  			case "member" => (fields(0), fields(1))
  		}
  	}

  	def safe[S, T](f : S => T) : S => Either[(S, Exception), T] = {                       // wrap the function with Either return
		new Function[S, Either[(S, Exception), T]] with Serializable {
			def apply(s : S) : Either[(S, Exception), T] = {
				try { 
					Right(f(s))
				} catch {
				  case e: Exception => Left((s, e))
				}
			}
		}
	}	
}
