import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object ChurnPredictionDT {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSessionCreate.createSession("ChurnPredictionRandomForest")
    import spark.implicits._
    val dTree = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(1234567L)
    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(PipelineConstruction.ipindexer,
        PipelineConstruction.labelindexer,
        PipelineConstruction.assembler,
        dTree))
    // Search through decision tree's maxDepth parameter for best model
    var paramGrid = new ParamGridBuilder()
      .addGrid(dTree.impurity, "gini" :: "entropy" :: Nil)
      .addGrid(dTree.maxBins, 3 :: 5 :: 9 :: 15 :: 23 :: 31 :: Nil)
      .addGrid(dTree.maxDepth, 5 :: 10 :: 15 :: 20 :: 25 :: 30 :: Nil)
      .build()
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")

    // Set up 10-fold cross validation
    val numFolds = 10
    val crossval = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)

    val cvModel = crossval.fit(Preprocessing.trainDF)

    val bestModel = cvModel.bestModel
    println("The Best Model and Parameters:\n--------------------")
    println(bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(3))

    bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      .stages(3)
      .extractParamMap

    val treeModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      .stages(3)
      .asInstanceOf[DecisionTreeClassificationModel]

    println("Learned classification tree model:\n" + treeModel.toDebugString)      
    println("Feature 11:" + Preprocessing.trainDF.select(PipelineConstruction.featureCols(11)))
    println("Feature 3:" + Preprocessing.trainDF.select(PipelineConstruction.featureCols(3)))

    val predictions = cvModel.transform(Preprocessing.testSet)
    predictions.show(10)

    val result = predictions.select("label", "prediction", "probability")
    val resutDF = result.withColumnRenamed("prediction", "Predicted_label")
    resutDF.show(10)

    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)
    evaluator.explainParams()

    val predictionAndLabels = predictions
      .select("prediction", "label")
      .rdd.map(x => (x(0).asInstanceOf[Double], x(1)
        .asInstanceOf[Double]))

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val areaUnderPR = metrics.areaUnderPR
    println("Area under the precision-recall curve: " + areaUnderPR)

    val areaUnderROC = metrics.areaUnderROC
    println("Area under the receiver operating characteristic (ROC) curve: " + areaUnderROC)

    /*
    val precesion = metrics.precisionByThreshold()
    println("Precision: "+ precesion.foreach(print))

    val recall = metrics.recallByThreshold()
    println("Recall: "+ recall.foreach(print))

    val f1Measure = metrics.fMeasureByThreshold()
    println("F1 measure: "+ f1Measure.foreach(print))
										 * 
*/
    val lp = predictions.select("label", "prediction")
    val counttotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / counttotal.toDouble
    val ratioCorrect = correct.toDouble / counttotal.toDouble
    val truep = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count() / counttotal.toDouble
    val truen = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count() / counttotal.toDouble
    val falsep = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble
    val falsen = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count() / counttotal.toDouble

    println("Total Count: " + counttotal)
    println("Correct: " + correct)
    println("Wrong: " + wrong)
    println("Ratio wrong: " + ratioWrong)
    println("Ratio correct: " + ratioCorrect)
    println("Ratio true positive: " + truep)
    println("Ratio false positive: " + falsep)
    println("Ratio true negative: " + truen)
    println("Ratio false negative: " + falsen)
  }
}