package spark.jobserver

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalactic._
import org.slf4j.LoggerFactory

import scala.util.Try
import spark.jobserver.api.{SparkJob => NewSparkJob, _}

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

/**
  * This is the same WordCountExample above but implementing the new SparkJob API.  A couple things
  * to notice:
  * - runJob no longer needs to re-parse the input.  The split words are passed straight to RunJob
  * - The output of runJob is typed now so it's more type safe
  * - the config input no longer is mixed with context settings, it purely has the job input
  * - the job could parse the jobId and other environment vars from JobEnvironment
  */
case class NamedRDD[T](rdd: RDD[T], forceComputation: Boolean, storageLevel: StorageLevel)
  extends  NamedObject
class RDDPersister[T] extends NamedObjectPersister[NamedRDD[T]] {
  override def persist(namedObj: NamedRDD[T], name: String) {
    namedObj match {
      case NamedRDD(rdd, forceComputation, storageLevel) =>
        require(!forceComputation || storageLevel != StorageLevel.NONE,
          "forceComputation implies storageLevel != NONE")
        rdd.setName(name)
        rdd.getStorageLevel match {
          case StorageLevel.NONE => rdd.persist(storageLevel)
          case currentLevel => rdd.persist(currentLevel)
        }
        // perform some action to force computation of the RDD
        if (forceComputation) rdd.count()
    }
  }

  /**
    * Calls rdd.persist(), which updates the RDD's cached timestamp, meaning it won't get
    * garbage collected by Spark for some time.
    * @param rdd the RDD
    */
  override def refresh(namedRDD: NamedRDD[T]): NamedRDD[T] = namedRDD match {
    case NamedRDD(rdd, _, _) =>
      rdd.persist(rdd.getStorageLevel)
      namedRDD
  }

  override def unpersist(namedRDD: NamedRDD[T]) {
    namedRDD match {
      case NamedRDD(rdd, _, _) =>
        rdd.unpersist(blocking = false)
    }
  }
}

object WordCountExampleNewApiSelfCache extends NewSparkJob {
  type JobData = Seq[String]
//  type JobOutput = RDD[(String, Long)]
  type JobOutput = collection.Map[String, Long]
  val logger = LoggerFactory.getLogger(getClass)

  def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
//    val cacheId: String = getCacheId(sc, runtime, data)
//    val t1 = System.nanoTime
//    val rdd: NamedRDD[(String, Long)] = runtime.namedObjects.getOrElseCreate(cacheId,
//      {
//        NamedRDD(sc.parallelize(data).map(x => (x, 1L)).reduceByKey(_ + _),
//          false,
//          StorageLevel.MEMORY_AND_DISK)
//      })(runtime.namedObjects.defaultTimeout, new RDDPersister[(String, Long)])
//    val duration = (System.nanoTime - t1) / 1e9d
//    logger.info(s"This job $runtime.jobId with cache[$cacheId] took $duration")
//    rdd.rdd

    val cacheId: String = getCacheId(sc, runtime, data)
    val rdd: NamedRDD[String] = runtime.namedObjects.getOrElseCreate(cacheId,
      {
        NamedRDD(sc.parallelize(data),
          false,
          StorageLevel.MEMORY_AND_DISK)
      })(runtime.namedObjects.defaultTimeout, new RDDPersister[String])
    rdd.rdd.countByValue
  }

  def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
  JobData Or Every[ValidationProblem] = {
    Try(config.getString("input.string").split(" ").toSeq)
      .map(words => Good(words))
      .getOrElse(Bad(One(SingleProblem("No input.string param new SparkJob"))))
  }
}