package org.apache.flink.quickstart

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import util.Properties
import org.apache.flink.core.fs.FileSystem.WriteMode
import scala.concurrent.{Future, ExecutionContext => ConcurrentExecutionContext, Await}
import scala.concurrent.duration._
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import org.slf4j.LoggerFactory
import org.slf4j.Logger

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val executionConfig = env.getConfig
      // https://ci.apache.org/projects/flink/flink-docs-master/apis/common/#execution-configuration
    executionConfig.disableSysoutLogging()
    
    // Logging
      // https://ci.apache.org/projects/flink/flink-docs-master/internals/logging.html
    val logger = LoggerFactory.getLogger(WordCount.getClass)
    logger.warn("Starting program")
    
    // Cannot use this because launching actions doesn't seem to be thread safe, 
    // see comment at the end of the file
    // execution context for futures to call actions asynchronously
    //implicit val concExecutionContext = ConcurrentExecutionContext.fromExecutor(new ThreadPoolExecutor(2, 4, 5000,  TimeUnit.MILLISECONDS,
    //            new LinkedBlockingQueue[Runnable]()))

    // get input data
    val text: DataSet[String] = env.fromElements("Oh To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,",
      "Whenever the winter winds, become too strong, I concentrate on you")

    val words: DataSet[String] = text.flatMap { line => 
      val ret = line.toLowerCase.split("\\W+")
      ret
    }
    
    val wordsPairs: DataSet[(String, Int)] = words.map { (_, 1) }
    // note according to the docs "This will not create a new DataSet, it will just attach 
    // the tuple field positions which will be used for grouping when executing a grouped operation."
    // which is confusing because the return type is different: maybe it means that this 
    // operation is lazy?
    val groupedCountPairs: GroupedDataSet[(String, Int)] = wordsPairs.groupBy(0)
    // sum is a custom functionality, probably optimized, that is declared in the
    // Enum org.apache.flink.api.java.aggregation.Aggregations
    val wordCounts: AggregateDataSet[(String, Int)]  = groupedCountPairs.sum(1)
    
    val word7Groups: GroupedDataSet[(String, Int)] = wordsPairs.groupBy {_ match { case (s, i) =>
      // see y below to see the BIG difference to Spark here
      s.length % 7
    }}
    // x.sum(1).print() error "Aggregate does not support grouping with KeySelector functions, yet."
    // Defining a custom aggregation is quite natural though. Note also the result is 
    // a regular DataSet, not an AggregateDataSet
    val word7Counts: DataSet[(String, Int)] = word7Groups.reduceGroup{ group => 
      // NOTE the group are not defined by the key in the definition of word7Groups, the fact that
      // this is a DataSet of pairs has nothing to do with the grouping. See word7GroupsAlt below
      // This key is not usable in practice, but just playing here
      group.reduce { (kC1, kC2) => (kC1, kC2) match {
        case ((key1, count1), (key2, count2)) => 
          (key1 + "-" + key2, count1 + count2)
      }}
      // this implementation is more declarative and equivalent in the sense that it
      // computes the same, but less efficient because it materializes the whole iterator
      // in memory, so it's more similar to what you'd do in Spark. Cool to have a choice here
//      val groupSeq = group.toSeq
//      val key = groupSeq.map(_._1).mkString("-")
//      val valsSum = groupSeq.map(_._2).reduce(_+_)
//      (key, valsSum)
    }
    //val word7CountsPrinted = Future { word7Counts.print() }
    word7Counts.print()
    
    // Note this makes more sense than word7Counts: as we don't need a key or field index for grouping
    // (we can't, but we don't need to) we can have a grouped dataset where the groups are not expressed
    // in the type, so there is no need of using pairs like in Spark
    val word7GroupsAlt: GroupedDataSet[String] = words.groupBy{ s => s.length % 7}
    val word7CountsAlt: DataSet[(String, Int)] = word7GroupsAlt.reduceGroup { group =>
      // this is basically the same we did for word7Count but mapping words to pairs 
      // with 1 inside the closure instead of outside. An optimized implementation 
      // that avoids the intermediate map using a var for counting could also be easily 
      // written here, again much flexibility! This is even better because we have another
      // overload that passes a Collector that can be used to emit more than one aggregation
      // per group
      group
        .map{(_, 1)}
        // in fact this is a copy paste of the aggregation for word7Counts
        .reduce{ (kC1, kC2) => (kC1, kC2) match {
          case ((key1, count1), (key2, count2)) => 
            (key1 + "-" + key2, count1 + count2)
        }}
    }
    // val word7CountsAltPrinted = Future { word7CountsAlt.print() }
    word7CountsAlt.print()
    
    //env.fromCollection(1 to 1000).groupBy{x => x % 7}.reduceGroup{_.sum }.collect()
    env.fromCollection(1 to 1000).groupBy{x => x % 7}.reduceGroup{xs => 
      val first = xs.next()
      (first % 7, 1 + xs.length)
    }.collect()
    
    val tupleSum = env.fromCollection(1 to 1000).groupBy{ x => (x%3, x%4) }.reduceGroup{_.sum}.collect()
    println(s"tupleSum: ${tupleSum.mkString(",")}")
    
    // execute and print result
    //val wordCountsPrinted = Future { wordCounts.print() } 
    wordCounts.print() // this forces execution
    
    val xs = env.fromCollection(1 to 1000)
    // NOTE we can reduceGroup on a non grouped dataset, using the whole dataset as a 
    // single group. Similarly when reduce() is applied to a grouped dataset then each
    // different group is reduced. So reduce() and groupReduce() are different formulations
    // of the same
    println(s"reduceGroup on non grouped: ${xs.reduceGroup(_.sum).collect.mkString(",")}")
    println(s"reduceGroup on grouped: ${xs.groupBy{_%2}.reduceGroup(_.sum).collect.mkString(",")}")
    println(s"reduce on non grouped: ${xs.reduce(_+_).collect.mkString(",")}")
    println(s"reduce on grouped: ${xs.groupBy{_%2}.reduce(_+_).collect.mkString(",")}")
    
    println(s"max value in each modulo class: ${xs.groupBy{_ % 7}
                                                  .sortGroup(identity[Int] _, Order.DESCENDING)
                                                  .reduceGroup{cs => 
                                                     val max = cs.next
                                                     (max % 7, max)
                                                   }.collect.mkString(",")}")

    import scala.math.max
    val xsPlus = xs.map( new RichMapFunction[Int, Int] {
       def map(x: Int): Int = x+1
       override def open(parameters: Configuration) {
         logger.warn(s"starting f with parameters $parameters")
         logger.warn("""this is a nice place for opening a database 
           connection, or creating a Jackson mapper, to say something""")
       }
       
       override def close() {
         logger.warn(s"closing f")
       }
     })
     
    val maxXsPlus = xsPlus.reduce((x, y) => max(x, y)).collect()(0)
    println(s"maxXsPlus: $maxXsPlus")
    // note the equivalent of RDD.take is trivially implemented by 
    // a reduceGroup(_.take(n).toSeq) followed by collect()(0): add to 
    // extra methods with a implicit class?
    val takeXsPlus = xsPlus.reduceGroup(_.take(4).toSeq).collect()(0)
    println(s"takeXsPlus ${takeXsPlus.mkString(",")}")
                                                   
    
    // await for all futures
//    val pendingActions = List(wordCountsPrinted, word7CountsPrinted, word7CountsAltPrinted)
//    val pendingActionsFinished = Future.fold(pendingActions)(Unit){ (u1, u2) => 
//      println("pending action finished")
//      Unit
//    }
//    Await.result(pendingActionsFinished, 10 seconds)
    
    // In general all data sink methods are lazy and are only executed when env.execute() is 
    // called. Non lazy data sink methods are documented as such in the scaladocs, but are
    // basically those that return data to the client process, like collect, print and count
      // this creates a folder WordCounts at the root of the project
    wordCounts.writeAsCsv("WordCounts", Properties.lineSeparator, ",", WriteMode.OVERWRITE)
    env.execute("Counting words")
   
    /*
     * Launching concurrent actions in futures lead to ConcurrentModificationException at the
     * call to env.execute(), and ConcurrentModificationException at the calls to print() for
     * a DataSet, at least in local mode execution. TODO study if there is some thread safe
     * way to concurrently launch actions
     * 
Exception in thread "main" java.util.ConcurrentModificationException
	at java.util.ArrayList$Itr.checkForComodification(Unknown Source)
	at java.util.ArrayList$Itr.next(Unknown Source)
	at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:50)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:994)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:961)
	at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:84)
	at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:855)
	at org.apache.flink.api.java.DataSet.collect(DataSet.java:410)
	at org.apache.flink.api.java.DataSet.print(DataSet.java:1605)
	at org.apache.flink.api.scala.DataSet.print(DataSet.scala:1615)
	at org.apache.flink.quickstart.WordCount$$anonfun$3.apply$mcV$sp(WordCount.scala:116)
	at org.apache.flink.quickstart.WordCount$$anonfun$3.apply(WordCount.scala:116)
	at org.apache.flink.quickstart.WordCount$$anonfun$3.apply(WordCount.scala:116)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
	at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.lang.Thread.run(Unknown Source)
     * 
     * */
    
    
  }
}
