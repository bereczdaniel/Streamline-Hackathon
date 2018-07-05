package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic
import eu.streamline.hackathon.flink.scala.job.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.util.Random

class TrainAndEvalWorkerLogic(learningRate: Double, numFactors: Int, rangeMin: Double, rangeMax: Double, k: Int, negativeSampleRate: Int) extends WorkerLogic {

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  lazy val SGDUpdater = new SGDUpdater(learningRate)
  lazy val workerId: Int = getRuntimeContext.getIndexOfThisSubtask

  val model = new mutable.HashMap[ItemId, Parameter]()
  val requestQueue =  new mutable.HashMap[UserId, mutable.Queue[EvaluationRequest]]()
  def itemIds: Array[ItemId] = model.keySet.toArray


  override def flatMap2(value: Types.WorkerInput, out: Collector[Either[Types.ParameterServerOutput, Types.Message]]): Unit = {
    value match {
      case eval: EvaluationRequest =>
        requestQueue.getOrElseUpdate(
          eval.userId,
          mutable.Queue[EvaluationRequest]()
        ).enqueue(eval)

        out.collect(Right(Pull(eval.userId, eval.evaluationId.toInt)))

      case _ =>
        throw new NotSupportedWorkerInput
    }
  }



  override def flatMap1(value: Types.PullAnswer, out: Collector[Either[Types.ParameterServerOutput, Types.Message]]): Unit = {
    val userVector = value.parameter
    val topK: TopK = generateLocalTopK(userVector)

    try{
      val request = requestQueue(value.targetId).dequeue()

      val itemVector = model.getOrElseUpdate(request.itemId, factorInitDesc.open().nextFactor(request.itemId))

      val userDelta: Parameter = train(userVector, request, itemVector)
      out.collect(Right(Push(value.targetId, userDelta)))
      out.collect(Left(EvaluationOutput(request.itemId, request.evaluationId, topK)))
    }
    catch {
      case _ : NoSuchElementException =>
        out.collect(Left(EvaluationOutput(-1, value.workerSource, topK)))
    }
  }

  def generateLocalTopK(userVector: Parameter): TopK = {
    model
      .map(item => (item._1, dotProduct(userVector, item._2)))
      .toList
      .sortBy(-_._2)
      .take(k)
  }

  def train(userVector: Parameter, request: EvaluationRequest, itemVector: Parameter): Parameter = {
    val negativeUserDelta = calculateNegativeSamples(Some(request.itemId), userVector)
    val (positiveUserDelta, positiveItemDelta) = SGDUpdater.delta(request.rating, userVector, itemVector)

    model.update(request.itemId, vectorSum(itemVector, positiveItemDelta))
    vectorSum(negativeUserDelta, positiveUserDelta)
  }

  def calculateNegativeSamples(itemId: Option[ItemId], userVector: Parameter): Parameter = {
    val possibleNegativeItems =
      itemId match {
        case Some(id) => itemIds.filterNot(_ == id)
        case None     => itemIds
      }

    (0 until  math.min(negativeSampleRate, possibleNegativeItems.length))
      .foldLeft(new Parameter(numFactors))((vector, _) => {
        val negItemId = possibleNegativeItems(Random.nextInt(possibleNegativeItems.length))
        val negItemVector = model(negItemId)

        val (userDelta, itemDelta) = SGDUpdater.delta(0.0, userVector, negItemVector)
        model(negItemId) = vectorSum(itemDelta, negItemVector)
        vectorSum(userDelta, vector)
      })
  }
}
