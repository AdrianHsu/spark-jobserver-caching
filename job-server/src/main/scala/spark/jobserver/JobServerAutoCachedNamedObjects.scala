package spark.jobserver

import akka.actor.ActorSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory
import spray.caching.{Cache, LruCache}
import spray.util._


/**
  * An implementation of [[NamedObjects]] API for the Job Server.
  * Note that this contains code that executes on the same thread as the job.
  * Uses spray caching for cache references to named objects and to
  * avoid that the same object is created multiple times
  */
case class CacheInfo(
                      var cached: Boolean,
                      var totalCalls: Int,
                      var estimatedProcessingTime: Option[Double] = None,
                      var nProfiles: Option[Int] = None,
                      var storageSize: Option[Long] = None)

class JobServerAutoCachedNamedObjects(system: ActorSystem) extends NamedObjects {

  val logger = LoggerFactory.getLogger(getClass)

  implicit val ec: ExecutionContext = system.dispatcher

  val config = system.settings.config

  val decayRate = config.getDouble("spark.jobserver.named-object-decay-rate")
  val epochSize = config.getInt("spark.jobserver.named-object-epoch-size")
  val budget = config.getLong("spark.jobserver.named-object-cache-size")


  private var currentProfile: scala.collection.mutable.Map[String, CacheInfo] =
    scala.collection.mutable.Map[String, CacheInfo]()
  private var updateProfile: scala.collection.mutable.Map[String, CacheInfo] =
    scala.collection.mutable.Map[String, CacheInfo]()
  private var jobsProcessed: Int = 0

  // Default timeout is 60 seconds. Hopefully that is enough
  // to let most RDD/DataFrame generator functions finish.
  val defaultTimeout = FiniteDuration(
    config.getDuration("spark.jobserver.named-object-creation-timeout", SECONDS), SECONDS)

  // we must store a reference to each NamedObject even though only its ID is used here
  // this reference prevents the object from being GCed and cleaned by sparks ContextCleaner
  // or some other GC for other types of objects
  private val namesToObjects: Cache[NamedObject] = LruCache()

  override def cachedCollect[T](name: String, objGen: => RDD[T]): Array[T] = {
    if (jobsProcessed==epochSize) {
      jobsProcessed = 0
      recomputeCacheAssignments()
    }

    jobsProcessed+=1

    val rddReturnOption = get[NamedRDD[T]](name)
    rddReturnOption match {
      case Some(namedRdd: NamedRDD[T]) => {
        val output = namedRdd.rdd.collect()

        //uncache the file if we need to
        currentProfile.get(name) match {
          case Some(CacheInfo(cached, _, _, _, _)) =>
            if (!cached) destroy(namedRdd, name)(new RDDPersister[T])
          case None => destroy(namedRdd, name)(new RDDPersister[T])
        }

        incrementCached(name)
        output
      }
      case None => {
        val rdd: RDD[T] = objGen

        //cache if we need to
        currentProfile.get(name) match {
          case Some(CacheInfo(cached, _, _, _, _)) => {
            if (cached) {
              createObject(
                NamedRDD(rdd, false, StorageLevel.MEMORY_ONLY), name)(new RDDPersister[T]).apply()
            }
          }
          case None =>
        }

        val t = System.nanoTime()
        val output = rdd.collect()
        val computeTime = System.nanoTime() - t
        val size = SizeEstimator.estimate(output)

        incrementComputed(name, computeTime, size)
        output
      }
    }
  }

  private def incrementCached(name: String){
    updateProfile.get(name) match {
      case Some(previous: CacheInfo) => {
        updateProfile+=(name -> CacheInfo(
          previous.cached,
          previous.totalCalls + 1,
          previous.estimatedProcessingTime,
          previous.nProfiles,
          previous.storageSize))
      }
      case None => {
        updateProfile+=(name -> CacheInfo(false, 1))
      }
    }
  }

  private def incrementComputed(name: String, computeTime: Long, size: Long){
    updateProfile.get(name) match {
      case Some(previous: CacheInfo) => {
        val newEstimatedProcessingTime =
          if (!previous.estimatedProcessingTime.isEmpty && !previous.nProfiles.isEmpty) {
            ((previous.estimatedProcessingTime.get * previous.nProfiles.get) + computeTime) /
              (previous.nProfiles.get + 1)
          } else { computeTime }

        updateProfile+=(name-> CacheInfo(
          false,
          previous.totalCalls + 1,
          Some(newEstimatedProcessingTime),
          Some(1 + previous.nProfiles.getOrElse(0)),
          Some(size)
        ))
      }
      case None => {
        updateProfile+=(name -> CacheInfo(
          false,
          1,
          Some(computeTime),
          Some(1),
          Some(size)
        ))
      }
    }
  }

  private def recomputeCacheAssignments(){
    combineProfiles()
    assignCaching(budget)
  }

  private def combineProfiles(): Unit = {
    for ((name, value) <- updateProfile){
      currentProfile.get(name) match {
        case None => currentProfile += (name -> value)
        case Some(previous: CacheInfo) => {
          val impact = 1 - decayRate
          currentProfile += (name -> CacheInfo(
            false,
            ((decayRate * previous.totalCalls) + (impact * value.totalCalls)).asInstanceOf[Int],
            if (previous.estimatedProcessingTime.isEmpty && value.estimatedProcessingTime.isEmpty) {
              None
            } else { Some(value.estimatedProcessingTime.getOrElse(previous.estimatedProcessingTime.get))},
            if (previous.nProfiles.isEmpty && value.nProfiles.isEmpty) {
              None
            } else { Some(value.nProfiles.getOrElse(previous.nProfiles.get))},
            if (previous.storageSize.isEmpty && value.storageSize.isEmpty) {
              None
            } else { Some(value.storageSize.getOrElse(previous.storageSize.get))}
          ))
        }
      }
    }
  }

  private def assignCaching(budget: Long) {
    var budgetLeft = budget
    val sorted = currentProfile.toSeq.sortWith((kv1, kv2) => {
      kv1._2.estimatedProcessingTime.getOrElse(0.0) * kv1._2.nProfiles.getOrElse(0).asInstanceOf[Double] >
        kv2._2.estimatedProcessingTime.getOrElse(0.0) * kv2._2.nProfiles.getOrElse(0).asInstanceOf[Double]
    })
    for(kv <- sorted) {
      kv._2.storageSize match {
        case Some(size) =>
          if (budgetLeft - size > 0) {}
          budgetLeft -= size
          currentProfile += (kv._1 -> CacheInfo(
            true,
            kv._2.totalCalls,
            kv._2.estimatedProcessingTime,
            kv._2.nProfiles,
            kv._2.storageSize
          ))
        case None =>
      }
    }
  }

//  override def cachedCollect[T](name: String, objGen: => RDD[T]): Array[T] = {
//    currentNJobs+=1
//    val t1 = System.nanoTime()
//    val rdd: RDD[T] = objGen
//    val t2 = System.nanoTime()
//    val output = rdd.collect()
//    val t3 = System.nanoTime()
//    val computeTime = System.nanoTime() - t1
//    val size = SizeEstimator.estimate(output)
//    val t4 = System.nanoTime()
//    logger.info(s"Time in cacheCollect: " +
//      s"RDD[T] gen:[${(t2-t1)/ 1e9}]," +
//      s"Collecting:[${(t3-t2)/ 1e9}], " +
//      s"Compute stats:[${(t4-t3)/ 1e9}], " +
//      s"Total of cachedCollect:[${(t4-t1)/ 1e9}]")
//    output
//  }

  override def getOrElseCreate[O <: NamedObject](name: String, objGen: => O)
                                                (implicit timeout: FiniteDuration = defaultTimeout,
                                                 persister: NamedObjectPersister[O]): O = {
    logger.info(s"In new nameObjects")
    val obj = cachedOp(name, createObject(objGen, name)).await(timeout).asInstanceOf[O]
    logger.info(s"Named object [$name] of type [${obj.getClass.toString}] created")
    obj
  }

  // wrap the operation with caching support
  // (providing a caching key)
  private def cachedOp[O <: NamedObject](name: String, f: () => O): Future[NamedObject] =
    namesToObjects(name) {
      logger.info("Named object [{}] not found, starting creation", name)
      f()
    }

  private def createObject[O <: NamedObject](objGen: => O, name: String)
                                            (implicit persister: NamedObjectPersister[O]): () => O = {
    () =>
    {
      val namedObj: O = objGen
      logger.info(s"Named object [$name] being persisted")
      persister.persist(namedObj, name)
      namedObj
    }
  }

  override def get[O <: NamedObject](name: String)
                                    (implicit timeout : FiniteDuration = defaultTimeout): Option[O] = {
    namesToObjects.get(name).map(_.await(timeout).asInstanceOf[O])
  }

  override def update[O <: NamedObject](name: String, objGen: => O)
                                       (implicit timeout : FiniteDuration = defaultTimeout,
                                        persister: NamedObjectPersister[O]): O = {
    get(name) match {
      case None => {}
      case Some(_) => forget(name)
    }
    //this does not work when the old object is not of the same type as the new one
    //  get(name).foreach(_ => destroy(name))
    getOrElseCreate(name, objGen)
  }

  def destroy[O <: NamedObject](objOfType: O, name: String)
                               (implicit persister: NamedObjectPersister[O]) {
    namesToObjects remove(name) foreach(f => f onComplete {
      case Success(obj) =>
        persister.unpersist(obj.asInstanceOf[O])
      case Failure(t) =>
    })
  }

  override def forget(name: String) {
    namesToObjects remove (name)
  }

  override def getNames(): Iterable[String] = {
    namesToObjects.keys match {
      case answer: Iterable[String] @unchecked => answer
    }
  }

}
