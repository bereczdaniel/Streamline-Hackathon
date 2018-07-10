package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic
import eu.streamline.hackathon.flink.scala.job.parameter.server.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages._
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages._
import eu.streamline.hackathon.flink.scala.job.parameter.server.pruning._
import eu.streamline.hackathon.flink.scala.job.parameter.server.pruning.LEMPPruningFunctions._
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Vector
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random
import scala.util.control.Breaks._

class TrainAndEvalWorkerLogic(learningRate: Double, numFactors: Int, negativeSampleRate: Int,
                              rangeMin: Double, rangeMax: Double,
                              workerK: Int, bucketSize: Int, pruningStrategy: LEMPPruningStrategy = LI(5, 2.5)) extends WorkerLogic {

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  lazy val SGDUpdater = new SGDUpdater(learningRate)
  lazy val workerId: Int = getRuntimeContext.getIndexOfThisSubtask

  val model = new mutable.HashMap[ItemId, Vector]()
  val requestBuffer =  new mutable.HashMap[Long, EvaluationRequest]()
  def itemIds: Array[ItemId] = model.keySet.toArray


  val itemIdsDescendingByLength = new mutable.TreeSet[(Double, ItemId)]()(implicitly[Ordering[(Double, ItemId)]].reverse)

  override def flatMap2(value: WorkerInput, out: Collector[Either[ParameterServerOutput, Message]]): Unit = {
    value match {
      case eval: EvaluationRequest =>
        requestBuffer.update(eval.evaluationId, eval)

        out.collect(Right(Pull(eval.evaluationId.toInt, eval.userId)))

      case _ =>
        throw new NotSupportedWorkerInput
    }
  }



  override def flatMap1(value: PullAnswer, out: Collector[Either[ParameterServerOutput, Message]]): Unit = {
    val userVector = value.parameter
    val topK: TopK = generateLocalTopK(userVector, pruningStrategy)

    val _request = requestBuffer.get(value.destination)

    _request match {
      case None =>
        out.collect(Left(EvaluationOutput(-1, value.destination, topK, -1)))
      case Some(request) =>
        val itemVector = model.getOrElseUpdate(request.itemId, Vector(factorInitDesc.open().nextFactor(request.itemId)))

        val userDelta: Vector = train(userVector, request, itemVector)

        out.collect(Right(Push(value.source, userDelta)))
        out.collect(Left(EvaluationOutput(request.itemId, request.evaluationId, topK, request.ts)))
    }
  }

  def generateLocalTopK(userVector: Vector, pruningStrategy: LEMPPruningStrategy): TopK = {

    val topK = new mutable.PriorityQueue[(Double, ItemId)]()(implicitly[Ordering[(Double, ItemId)]].reverse)
    val buckets = itemIdsDescendingByLength.toList.grouped(bucketSize)

    val userVectorLength = userVector.length

    breakable{
      for(currentBucket <- buckets){
        if (!((topK.length < workerK) || (currentBucket.head._1 * userVectorLength > topK.head._1))){
          break()
        }
        val (focus, focusSet) = generateFocusSet(userVector, pruningStrategy)

        val candidates = pruneCandidateSet(topK, currentBucket, pruningStrategy, focus, focusSet, userVector)

        //TODO check math
        for (item <- candidates) {
          val userItemDotProduct = Vector.dotProduct(userVector, item._2)

          if (topK.size < workerK) {
            topK += ((userItemDotProduct, item._1))
          }
          else {
            if (topK.head._1 < userItemDotProduct) {
              topK.dequeue
              topK += ((userItemDotProduct, item._1))
            }
          }
        }

      }
    }
    topK
      .map(x => (x._2, x._1))
      .toList
  }

  def train(userVector: Vector, request: EvaluationRequest, itemVector: Vector): Vector = {
    val negativeUserDelta = calculateNegativeSamples(Some(request.itemId), userVector)
    val (positiveUserDelta, positiveItemDelta) = SGDUpdater.delta(request.rating, userVector.value, itemVector.value)

    val updatedItemVector = Vector.vectorSum(itemVector, Vector(positiveItemDelta))
    model.update(request.itemId, updatedItemVector)
    itemIdsDescendingByLength.add((updatedItemVector.length, request.itemId))
    Vector(Vector.vectorSum(negativeUserDelta, positiveUserDelta))
  }

  def calculateNegativeSamples(itemId: Option[ItemId], userVector: Vector): Parameter = {
    val possibleNegativeItems =
      itemId match {
        case Some(id) => itemIds.filterNot(_ == id)
        case None     => itemIds
      }

    (0 until  math.min(negativeSampleRate, possibleNegativeItems.length))
      .foldLeft(new Parameter(numFactors))((vector, _) => {
        val negItemId = possibleNegativeItems(Random.nextInt(possibleNegativeItems.length))
        val negItemVector = model(negItemId)

        val (userDelta, itemDelta) = SGDUpdater.delta(0.0, userVector.value, negItemVector.value)
        model(negItemId) = Vector(Vector.vectorSum(itemDelta, negItemVector.value))
        Vector.vectorSum(userDelta, vector)
      })
  }

  def generateFocusSet(userVector: Vector, pruning: LEMPPruningStrategy): (ItemId, Array[ItemId]) = {
    val focus = ((1 until userVector.value.length) :\ 0) { (i, f) =>
      if (userVector.value(i) * userVector.value(i) > userVector.value(f) * userVector.value(f))
        i
      else
        f
    }

    // focus coordinate set for incremental pruning test
    val focusSet = Array.range(0, userVector.value.length - 1)
      .sortBy{ x => -userVector.value(x) * userVector.value(x) }
      .take(pruning match {
        case INCR(x)=> x
        case LI(x, _)=> x
        case _=> 0
      })

    (focus, focusSet)
  }

  def pruneCandidateSet(topK: mutable.PriorityQueue[(Double, ItemId)], currentBucket: List[(Double, Types.ItemId)],
                        pruning: LEMPPruningStrategy,
                        focus: Int, focusSet: Array[Int],
                        userVector: Vector): List[(ItemId, Vector)] = {
    val theta = if (topK.length < workerK) 0.0 else topK.head._1
    val theta_b_q = theta / (currentBucket.head._1 * userVector.length)
    val vectors = currentBucket.map(x => (x._2, model(x._2)))



    vectors.filter(
      pruning match {
      case LENGTH() => lengthPruning(theta / userVector.length)
      case COORD() => coordPruning(focus, userVector, theta_b_q)
      case INCR(_) => incrPruning(focusSet, userVector, theta)
      case LC(threshold) =>
        if (currentBucket.head._1 > currentBucket.last._1 * threshold)
          lengthPruning(theta / userVector.length)
        else
          coordPruning(focus, userVector, theta_b_q)
      case LI(_, threshold) =>
        if (currentBucket.head._1 > currentBucket.last._1 * threshold)
          lengthPruning(theta / userVector.length)
        else
          incrPruning(focusSet, userVector, theta)
    })
  }
}
