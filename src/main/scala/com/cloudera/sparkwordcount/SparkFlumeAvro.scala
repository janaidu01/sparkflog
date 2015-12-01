/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.cloudera.sparkwordcount

//import org.joda.time.DateTime
import org.joda.time._

import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
 
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.util.IntParam
import java.net.InetSocketAddress

import org.apache.spark.util.Utils


//import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.json._
import org.apache.spark.sql.json.JsonRDD
//import org.apache.spark.sql.SQLContext.implicits._;
/**
 *  Produces a count of events received from Flume.
 *
 *  This should be used in conjunction with the Spark Sink running in a Flume agent. See
 *  the Spark Streaming programming guide for more details.
 *
 *  Usage: FlumePollingEventCount <host> <port>
 *    `host` is the host on which the Spark Sink is running.
 *    `port` is the port at which the Spark Sink is listening.
 *
 *  To run this example:
 *    `$ bin/run-example org.apache.spark.examples.streaming.FlumePollingEventCount [host] [port] `
 * object FlumePollingEventCount {
 */
object SparkFlumeAvro {

  def g(v:org.apache.spark.streaming.flume.SparkFlumeEvent) = v.event.toString()
      
  def main(args: Array[String]) {

//    if (args.length < 1) {
//      System.err.println(
//        "Usage: FlumePollingEventCount <host> ")
//      System.exit(1)
//    }
//    StreamingExamples.setStreamingLogLevels()
//  val Array(host) = args

    val host= "jprosser-finra-impala2-1.vpc.cloudera.com"
    val port=41415

    val batchInterval = Milliseconds(4000)

    // Create the context and set the batch size
    val sparkConf = new org.apache.spark.SparkConf().setAppName("FlumePollingEventCount")
    
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, batchInterval)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    // storage default = StorageLevel.MEMORY_AND_DISK_SER_2

    val stream = FlumeUtils.createPollingStream(ssc, host, port)

    // Print out the count of events received from this server in each batch
//    stream.count().map(cnt => "RECEIVED " + cnt + " flume events." ).print()

    val events = stream.map(e => e.event)
    val edata = events.map(event => new String(event.getBody.array))

//    edata.print()
//    val jsonData=events.map(event => event.getBody.array)
//      var lastSchema=Array[String]()

    val auditSchema =
       StructType(
         StructField("type",      StringType,false) ::
         StructField("allowed",   StringType,false) ::
         StructField("time",      LongType,false) ::
         StructField("service",   StringType,false) ::
         StructField("user",      StringType,false) ::
         StructField("ip",        StringType,false) ::
         StructField("op",        StringType,false) ::
         StructField("opText",    StringType,false) ::
         StructField("db",        StringType,false) ::
         StructField("table",     StringType,false) ::
         StructField("path",      StringType,true) ::
         StructField("objType",   StringType,false) ::
         StructField("privilege", StringType,true) ::
         StructField("status",    StringType,true) ::
         StructField("ingestTime",LongType,false) :: Nil)

	 val options= Map(("spark.sql.parquet.compression.codec","uncompressed"))

// No hive yet, so no external table.
//import org.apache.spark.sql.hive.HiveContext
//val hiveCtx = new HiveContext(sc)
//val rows = hiveCtx.sql("SELECT * FROM cdrs") 
// val firstRow = rows.first() 
// println(firstRow.getString(0))
//  val myTable = sqlContext.createExternalTable("queryEvents","json",auditSchema,options)


//    
//val tmpJsonRDD : JsonRD
      
      edata.foreachRDD(rdd => {
	 if(!rdd.partitions.isEmpty) {//rdd is a RDD of strings
	   
 	     val json2=sqlContext.jsonRDD(rdd) //json2 is a dataframe
	     val struct = json2.schema
//	     val newstruct=struct.merge(auditSchema)
	     struct.printTreeString() 

// SaveMode.Append


//	     val newSchema = json2.schema.fieldNames
//	     if(newSchema.deep==lastSchema.deep){
//	         print("no change to schema");
//	       }else{
//	         print("CHANGE to schema");
//		 lastSchema=newSchema
//
//	       }
//	     val finalDataFrame	=sqlContext.applySchema(json2,auditSchema)
//	     val finalDataFrame  = sqlContext.createDataFrame(json2, auditSchema)
	     
	     val today = Calendar.getInstance().getTime()
	     print("now is " +today)

	     //sqlContext.udf.register("getYear", (arg1: Long) => arg1.toString())

//    val time_col = sqlc.sql("select ts from mr").map(line => new DateTime(line(0)).toString("yyyy-MM-dd"))

// new org.joda.time.DateTime(1378607203*1000)

//	      def getYear(ts: String) = new DateTime(ts).toString("yyyy")
	      def getYear(ts: String) = new DateTime(ts).toString("yyyy")
	      def getMonth(ts: Long) = new DateTime(ts).toString("MM")
	      def getDay(ts: Long) = new DateTime(ts).toString("dd")

	      sqlContext.udf.register("getYear", getYear _)
	      sqlContext.udf.register("getMonth", getMonth _)
	      sqlContext.udf.register("getDay", getDay _)

	     json2.registerTempTable("auditdata")
 	     System.err.println(" about to call udf")
	   
	     val opText = sqlContext.sql("SELECT opText,getYear(time) from auditdata")

//	     val opText = sqlContext.sql("SELECT opText,getYear(substr(time,1,9)) from auditdata")
//	     val opText = sqlContext.sql("SELECT opText,time from auditdata")
//	     opText.map(t => " opText2: " + t(0) + " time: " + t(1) + " year: " + t(2) + " month: " + t(3) ).collect().foreach(println)
	     opText.map(t => " opText2: " + t(0) + " time: " + t(1) ).collect().foreach(println)


//	     myTable.write().mode(SaveMode.Append).partitionBy("year","month","day").format("parquet").



	 }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println