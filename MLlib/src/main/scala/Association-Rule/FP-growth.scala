/*本文档基于spark1.4 版本中的FPGrowth的频繁项，进一步计算出关联规则。
目前在sparkshell下运行，未打jar包。

数据集：
关系库的格式：
两列：ID Items

交易事务的格式：
每行：为一个同一用户购买的不同产品

注意：如果是数据集是关系库的格式，需要转换为交易事务的格式 
*/

./bin/spark-shell --driver-memory 5g

val rawData = sc.textFile("file:///tmp/pg_trans_2014.txt")
val rdd00 = rawData.map(_.split("\t")).map(a => (a(0), a(1)))
val rdd11 = rdd00.reduceByKey(_ + "," + _)
val rdd22 = rdd11.map{case (a, b) => b}
//rdd22.saveAsTextFile("file:///tmp/out00")
//val rdd22 = sc.textFile("file:///tmp/out00/part-all")
val examples = rdd22.map(_.split(",")).cache()
//rdd22.unpersist()

val totalExamples = examples.count()
val minSupport = 0.01

import org.apache.spark.mllib.fpm._
val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(10)
val model = fpg.run(examples)

//examples.unpersist()


println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
model.freqItemsets.collect().foreach { itemset =>
  println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq + ", " + (itemset.freq*1.0/totalExamples))
}

/*
model.freqItemsets.collect().foreach(itemset => println(itemset.items.mkString("(", ",", ")")))
model.freqItemsets.collect().foreach(itemset => println(itemset.freq))

model.freqItemsets.collect().foreach { itemset =>
  println(List(itemset.items.mkString("(", ",", ")") + ", " + itemset.freq + ", " + BigDecimal(itemset.freq*1.0/totalExamples).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble))
}
*/

val fpitems = 
for (itemset <- model.freqItemsets.collect())
  yield ((itemset.items.mkString(",") + "," + itemset.freq + "," +BigDecimal(itemset.freq*1.0/totalExamples).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble))

val fpitemsTrans = fpitems.map(_.split(","))
val items = fpitemsTrans.map(a => a.toList.take(a.length-2))
def distinctItems = items.flatten.distinct
val Support = fpitemsTrans.map(a => a(a.length-1).toFloat)
val Frequency = fpitemsTrans.map(a => a(a.length-2).toInt)
val itemsAndSupport = items.zip(Support)
val itemsMapSupport = items.zip(Support).toMap
val itemsMapFrequency = items.zip(Frequency).toMap
val minConfidence = 0.60

def isSafe(comA: List[String], comB: List[String]): Boolean = {
      if (comA.length < comB.length)
	    comA forall ( k => comB contains k)
	  else false
     }

def generList(comA: List[String], comB: List[String]) = {
     for {
	      i <- comB
	      if ! (comA contains i)
	  }
         yield i
     }	

def showGenerListSupport(comStr: List[String]): Double = itemsMapSupport.get(comStr) match {
     case Some(supp) => supp
     case None => 1.0
    }

case class fpResult(comA: List[String], comAB: List[String], comB: List[String], frequencyAB: Int, supportAB: Double, confA2B: Double, supportB: Double, liftB: Double)

val ruleCollect = 	 
for {
     i <- itemsAndSupport
     j <- itemsAndSupport
	 if isSafe(i._1, j._1)
	 if j._2 / i._2 > minConfidence
	 val gList = generList(i._1, j._1)
	 val gListSupport = showGenerListSupport(gList)
	 val j1Support = itemsMapSupport(j._1)
	 }
yield fpResult(i._1, j._1, gList, itemsMapFrequency(j._1), "%.3f".format(j1Support).toDouble, "%.3f".format(j._2 / i._2).toDouble, "%.3f".format(gListSupport).toDouble, "%.2f".format(j._2 / (i._2 * gListSupport)).toDouble ) 
//yield fpResult(i._1, j._1, gList, itemsMapFrequency(j._1), j._2 / i._2, gListSupport, j._2 / (i._2 * gListSupport) )  

//ruleCollect.map(a => (a.comA, a.comB, a.comAB, a.frequencyAB, a.supportAB, a.confA2B, a.supportB, a.liftB)).foreach(println)

println("Association Rule: A => B" + "\tABInstances" + "\tABSupport" + "\tA2BConfince" + "\tBlift")
ruleCollect.map(a => a.comA.mkString("[", "," ,"]") + " => " + a.comB.mkString("[", "," ,"]") + "\t"+ a.frequencyAB + "\t" + a.supportAB + "\t" + a.confA2B + "\t" + a.liftB).foreach(println)
