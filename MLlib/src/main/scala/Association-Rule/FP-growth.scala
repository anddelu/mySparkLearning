/**本文档基于spark1.4 版本中的FPGrowth的频繁项，进一步计算出关联规则。
   目前在sparkshell下运行，未打jar包。

  数据集：
  关系库的格式：
  两列：ID Items

  交易事务的格式：
  每行：为一个同一用户购买的不同产品

  注意：如果是数据集是关系库的格式，需要转换为交易事务的格式 
 */

//进入spark shell的环境，设置driver memory的大小，但这个值要根据你机器具体情况来设置，比如你机器本身就是32位的操作系统，肯定不能超过4g的
./bin/spark-shell --driver-memory 5g

//加载数据集，例子中给出数据集格式为关系库的格式
val rawData = sc.textFile("file:///tmp/pg_trans_2014.txt")

//将关系库格式转换为交易事务格式，可以选择保存在本地文件系统或者hdfs上
val rdd00 = rawData.map(_.split("\t")).map(a => (a(0), a(1)))
val rdd11 = rdd00.reduceByKey(_ + "," + _)
val rdd22 = rdd11.map{case (a, b) => b}
rdd22.saveAsTextFile("file:///tmp/out00")

//如果数据集格式为交易事务格式，直接加载，不需要上面的那些转换操作
//val rdd22 = sc.textFile("file:///tmp/out00/*")

//将数据处理成能够FP算法读取的格式
val examples = rdd22.map(_.split(",")).cache()    //缓存examples

val totalExamples = examples.count()             //计算出多少条交易事务性数据
val minSupport = 0.01                            //设置最小支持度

//导入必要的包，并训练模型
import org.apache.spark.mllib.fpm._
val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(10)
val model = fpg.run(examples)

//初步查看结果
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
//频繁项集合
val fpitems = 
for (itemset <- model.freqItemsets.collect())
  yield ((itemset.items.mkString(",") + "," + itemset.freq + "," +BigDecimal(itemset.freq*1.0/totalExamples).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble))
//对频繁项集合做转换，并得出频繁项（items）、对应的支持度（Support）、对应的出现次数（Frequency）、及需要的Map映射
val fpitemsTrans = fpitems.map(_.split(","))
val items = fpitemsTrans.map(a => a.toList.take(a.length-2))
def distinctItems = items.flatten.distinct                      //出现在频繁项集合中，单个不同元素项的集合
val Support = fpitemsTrans.map(a => a(a.length-1).toFloat)
val Frequency = fpitemsTrans.map(a => a(a.length-2).toInt)
val itemsAndSupport = items.zip(Support)
val itemsMapSupport = items.zip(Support).toMap                  //频繁项和对应支持度的映射
val itemsMapFrequency = items.zip(Frequency).toMap              //频繁项和对应出现次数的映射
val minConfidence = 0.60                                        //最小置信度

//定义满足产生关联规则条件的频繁项
def isSafe(comA: List[String], comB: List[String]): Boolean = {
      if (comA.length < comB.length)
	    comA forall ( k => comB contains k)
	  else false
     }
//产生关联规则，比如 A， AB作为频繁项，而B不是频繁项，能够满足minConfidence，则得到一条规则： A => B 
def generList(comA: List[String], comB: List[String]) = {
     for {
	      i <- comB
	      if ! (comA contains i)
	  }
         yield i
     }	
//对于产生的规则，找出相应的支持度，主要为计算提升度（lift），比如对于上述A => B，由于B不是频繁项，所以没在itemsMapSupport中，默认B的支持度为1.0，这里为了更快的计算，采用了简化的手法，其实也可以算出来的。
def showGenerListSupport(comStr: List[String]): Double = itemsMapSupport.get(comStr) match {
     case Some(supp) => supp
     case None => 1.0
    }
//定义case类 fpResult
case class fpResult(comA: List[String], comAB: List[String], comB: List[String], frequencyAB: Int, supportAB: Double, confA2B: Double, supportB: Double, liftB: Double)
//定义生成的规则
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

//ruleCollect.map(a => (a.comA, a.comB, a.comAB, a.frequencyAB, a.supportAB, a.confA2B, a.supportB, a.liftB)).foreach(println)
println("Association Rule: A => B" + "\tABInstances" + "\tABSupport" + "\tA2BConfince" + "\tBlift")
ruleCollect.map(a => a.comA.mkString("[", "," ,"]") + " => " + a.comB.mkString("[", "," ,"]") + "\t"+ a.frequencyAB + "\t" + a.supportAB + "\t" + a.confA2B + "\t" + a.liftB).foreach(println)

examples.unpersist()                            //释放examples的cache
