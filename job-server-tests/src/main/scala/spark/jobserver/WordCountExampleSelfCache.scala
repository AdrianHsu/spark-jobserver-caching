package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic._
import org.slf4j.LoggerFactory

import scala.util.Try
import spark.jobserver.api.{SparkJob => NewSparkJob, _}
import org.apache.spark.storage.RDDInfo


/**
  * A super-simple Spark job example that implements the SparkJob
  * trait and can be submitted to the job server.
  *
  * Set the config with the sentence to split or count:
  * input.string = "adsfasdf asdkf  safksf a sdfa"
  *
  * validate() returns SparkJobInvalid if there is no input.string
  */
object WordCountExampleSelfCache extends SparkJob {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName("WordCountExample")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param SparkJob"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    sc.parallelize(config.getString("input.string").split(" ").toSeq).countByValue
  }
}

object WordCountExampleNewApiSelfCache extends NewSparkJob {
  type JobData = Seq[String]
  type JobOutput = Array[(String, Long)]
  val logger = LoggerFactory.getLogger(getClass)

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
      //logger.info("#$ " + data.toString())
      val cacheId: String = getCacheId(sc, runtime, data)
      val t1 = System.nanoTime()
      val result = runtime.namedObjects.cachedCollect(cacheId,
        sc.parallelize(data).map(x => (x, 1L)).reduceByKey(_ + _)
      )
      val t2 = System.nanoTime()
      logger.info(s"Time took ${(t2-t1)/1e9}")
      result
//      val t1 = System.nanoTime
//    //TODO maybe adjust the RDDPersister
//      val wordCountNamedRdd: NamedRDD[(String, Long)] = runtime.namedObjects.getOrElseCreate(cacheId,
//        {
//          NamedRDD(sc.parallelize(data).map(x => (x, 1L)).reduceByKey(_ + _),
//            false,
//            StorageLevel.MEMORY_AND_DISK)
//        })(runtime.namedObjects.defaultTimeout, new RDDPersister[(String, Long)])
//      val duration = (System.nanoTime - t1) / 1e9d
//      logger.info(s"This job $runtime.jobId with cache[$cacheId] took $duration")
//      logger.info("#$ " + wordCountNamedRdd.rdd.toString())
////      val info = RDDInfo.fromRdd(wordCountNamedRdd.rdd)
////      logger.info("#$ Size is" +  info)
//      wordCountNamedRdd.rdd.collect()
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param new SparkJob"))))
  }
}