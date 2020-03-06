package bootstrap

import java.io._
import scala.io.Source
import scala.math._
import org.apache.spark._
import org.apache.spark.rdd._
import au.com.bytecode.opencsv.CSVParser

case class Budget (foodPercent:Double, totalExpense:Double, age:Int, householdSize:Int, townSize:Int, gender:String)

// Calculate the average food expenditure of person per month
object main extends App{
 
  override
  def main(args:Array[String]){ 
    var conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    var sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val data = readFile("BudgetFood.csv",sc)
    val budgetRDD = getData(data,sc)
    
    val population = calculateExpense(budgetRDD)
    val sample = population.sample(false,0.25,1)
    
    println("Population size: " + population.collect().size)
    println("---------------------------")
    
    val actualStats = getMeanAndVariance(population)
  
    var NoRepStats = getMeanAndVariance(sample)
    
    var supSample = sample.sample(true,1)
    var tempstates = getMeanAndVariance(supSample)
    var sumstates : Array[(Int,Double,Double)] =tempstates
    for(i <- 1 to 100) {
     supSample  = sample.sample(true,1)
     tempstates = getMeanAndVariance(supSample)
    sumstates   = addTupelsList(sumstates,tempstates)
    
    
    }
    
    val avgStates=devBy100(sumstates)
    
    println("ACTUAL")
    println("Group\tMean\tVariance")
    printStats(actualStats)
   
    
    println("---------------------------")
    println("ESTIMATE")
     println("Group\tMean\tVariance")
     printStats(    avgStates)
     
    println("---------------------------")
    println("ABSOLUTE ERROR PERCENTAGE")
     println("Group\tMean\tVariance")
     printStats(calcAccurcy(actualStats,avgStates))
     
  }
  
  def calcAccurcy (list:Array[(Int,Double,Double)],list2 : Array[(Int,Double,Double)] ): Array[(Int,Double,Double)]={
  
    var ret = new Array[((Int,Double,Double))](list.size)
   for(i <- 0 to list.size-1)
    {
     var temp= list(i)
     var temp2=list2(i)
     var difvar= (abs(temp._3-temp2._3)*100)/temp._3
     var difavg = (abs(temp._2-temp2._2)*100)/temp._2
     
     var sum = (temp._1,difavg,difvar) 
      ret(i)= sum
      
    }
    ret
    
  }
  
  def addTupelsList (list:Array[(Int,Double,Double)],list2 : Array[(Int,Double,Double)] ): Array[(Int,Double,Double)]={
  
    var ret = new Array[((Int,Double,Double))](list.size)
   for(i <- 0 to list.size-1)
    {
     var temp= list(i)
     var temp2=list2(i)
     var sumvar= temp._3+temp2._3
     var sumavg = temp._2+temp2._2
     
     var sum = (temp._1,sumavg,sumvar) 
      ret(i)= sum
      
    }
    ret
    
  }
   def devBy100 (list:Array[(Int,Double,Double)] ): Array[(Int,Double,Double)]={
  
    var ret = new Array[((Int,Double,Double))](list.size)
    
   for(i <- 0 to ret.size-1)
    {
     val temp= list(i)
     
     
     val res = (temp._1,temp._2/100,temp._3/100) 
      ret(i)= res
      
    }
    ret
    
  }
  
  def printStats(data:Array[(Int,Double,Double)]) = {
      val formatter = "%1.2f"
      data.foreach(tuple => println(tuple._1 + "\t" + formatter.format(tuple._2) + "\t" + formatter.format(tuple._3)))
  }

  def getMeanAndVariance(population:RDD[(Int,Double)]) : Array[(Int,Double,Double)] = {
     val stripe = population.groupByKey()
     val meanTuple = stripe.map(tuple => (tuple._1,tuple._2,mean(tuple._2)))
     meanTuple.map(tuple => (tuple._1,tuple._3,variance(tuple._2,tuple._3))).sortBy(_._1).collect()
  }
  
  def variance(list:Iterable[Double], mean:Double) : Double = {
     val list2 = list.map(x => pow((x-mean),2))
     list2.sum/list.size
  }
  
  def mean(list:Iterable[Double]) : Double = {
     list.sum*1d/list.size
  }
      
  def calculateExpense(dataArray:RDD[Budget]) : RDD[(Int,Double)] = {
      dataArray.map(budget => {
        (budget.townSize,budget.foodPercent) 
      })
  }
  
  def getData(ds:RDD[String],sc:SparkContext) : RDD[Budget] = {
    val firstLine = ds.first
     ds.filter(line => line!=firstLine)
       .map(line => getBudget(line))
  }
  
  def getBudget(line:String) : Budget = {
     val cols = line.split(",")
     Budget(cols(1).toDouble*100,cols(2).toInt,cols(3).toInt,cols(4).toInt,cols(5).toInt,cols(6))
  }
  
  def readFile(fileName:String, sc:SparkContext) : RDD[String] = {
    val currentDir = System.getProperty("user.dir")
    sc.textFile(currentDir+"/src/main/resources/" + fileName)
  }
}