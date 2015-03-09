package com.cloudera.spark.hbase

import org.apache.spark.api.java.JavaSparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.api.java.function.Function
import org.apache.hadoop.hbase.client.HConnection
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.api.java.function.FlatMapFunction
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.reflect.ClassTag

class JavaHBaseContext(@transient jsc: JavaSparkContext,
  @transient config: Configuration) extends Serializable {
  val hbc = new HBaseContext(jsc.sc, config)
  
  /**
   * A simple enrichment of the traditional Spark javaRdd foreachPartition.
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * @param javaRDD Original javaRdd with data to iterate over
   * @param f       Function to be given a iterator to iterate through
   *                the RDD values and a HConnection object to interact
   *                with HBase
   */
  def foreachPartition[T](javaRdd: JavaRDD[T],
    f: VoidFunction[(java.util.Iterator[T], HConnection)] ) = {
    
    hbc.foreachPartition(javaRdd.rdd, 
        (iterator:Iterator[T], hConnection) => 
          { f.call((iterator, hConnection))})
  } 
  
  def foreach[T](javaRdd: JavaRDD[T],
    f: VoidFunction[(T, HConnection)] ) = {
    
    hbc.foreachPartition(javaRdd.rdd, 
        (iterator:Iterator[T], hConnection) =>
          iterator.foreach(a => f.call((a, hConnection))))
          
          //{ f.call((iterator, hConnection))})
  }
  
  /**
   * A simple enrichment of the traditional Spark Streaming dStream foreach
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * @param JavaDStream Original DStream with data to iterate over
   * @param f           Function to be given a iterator to iterate through
   *                    the JavaDStream values and a HConnection object to
   *                    interact with HBase
   */
  def foreachRDD[T](javaDstream: JavaDStream[T],
    f: VoidFunction[(Iterator[T], HConnection)]) = {
    hbc.foreachRDD(javaDstream.dstream, (it:Iterator[T], hc: HConnection) => f.call(it, hc))
  }
  
    /**
   * A simple enrichment of the traditional Spark JavaRDD mapPartition.
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   *
   * @param JavaRdd Original JavaRdd with data to iterate over
   * @param mp      Function to be given a iterator to iterate through
   *                the RDD values and a HConnection object to interact
   *                with HBase
   * @return        Returns a new RDD generated by the user definition
   *                function just like normal mapPartition
   */
  def mapPartition[T,R](javaRdd: JavaRDD[T],
    mp: FlatMapFunction[(java.util.Iterator[T], HConnection),R] ): JavaRDD[R] = {
     
    def fn = (x: Iterator[T], hc: HConnection) => 
      asScalaIterator(
          mp.call((asJavaIterator(x), hc)).iterator()
        )
    
    JavaRDD.fromRDD(hbc.mapPartition(javaRdd.rdd, 
        (iterator:Iterator[T], hConnection:HConnection) => 
          fn(iterator, hConnection))(fakeClassTag[R]))(fakeClassTag[R])
  }  
  
    /**
   * A simple enrichment of the traditional Spark Streaming JavaDStream
   * mapPartition.
   *
   * This function differs from the original in that it offers the
   * developer access to a already connected HConnection object
   *
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   *
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   *
   * @param JavaDStream Original JavaDStream with data to iterate over
   * @param mp          Function to be given a iterator to iterate through
   *                    the JavaDStream values and a HConnection object to
   *                    interact with HBase
   * @return            Returns a new JavaDStream generated by the user
   *                    definition function just like normal mapPartition
   */
  def streamMap[T, U](javaDstream: JavaDStream[T],
      mp: Function[(Iterator[T], HConnection), Iterator[U]]): JavaDStream[U] = {
    JavaDStream.fromDStream(hbc.streamMap(javaDstream.dstream, 
        (it: Iterator[T], hc: HConnection) => 
         mp.call(it, hc) )(fakeClassTag[U]))(fakeClassTag[U])
  }
  
  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take JavaRDD
   * and generate puts and send them to HBase.
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaRDD   Original JavaRDD with data to iterate over
   * @param tableName The name of the table to put into
   * @param f         Function to convert a value in the JavaRDD 
   *                  to a HBase Put
   * @param autoFlush If autoFlush should be turned on
   */
  def bulkPut[T](javaDdd: JavaRDD[T], 
      tableName: String, 
      f: Function[(T), Put], 
      autoFlush: Boolean) {
    
    hbc.bulkPut(javaDdd.rdd, tableName, (t:T) => f.call(t), autoFlush)
  }
  
  /**
   * A simple abstraction over the HBaseContext.streamMapPartition method.
   *
   * It allow addition support for a user to take a JavaDStream and
   * generate puts and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaDStream Original DStream with data to iterate over
   * @param tableName   The name of the table to put into
   * @param f           Function to convert a value in 
   *                    the JavaDStream to a HBase Put
   * @autoFlush         If autoFlush should be turned on
   */
  def streamBulkPut[T](javaDstream: JavaDStream[T],
      tableName: String,
      f: Function[T,Put],
      autoFlush: Boolean) = {
    hbc.streamBulkPut(javaDstream.dstream, 
        tableName, 
        (t:T) => f.call(t),
        autoFlush)
  }

  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take RDD
   * and generate checkAndPuts and send them to HBase.
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param RDD       Original RDD with data to iterate over
   * @param tableName The name of the table to put into
   * @param f         Function to convert a value in the RDD to 
   *                  a HBase checkAndPut
   * @param autoFlush If autoFlush should be turned on
   */
  def bulkCheckAndPut[T](javaRdd: JavaRDD[T], 
      tableName: String, 
      f: Function[T,(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Put)], 
      autoFlush: Boolean) {
    
    hbc.bulkCheckAndPut(javaRdd.rdd, tableName, (t:T) => f.call(t), autoFlush)
  }
  
  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take a JavaRDD and
   * generate increments and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaRDD   Original JavaRDD with data to iterate over
   * @param tableName The name of the table to increment to
   * @param f         function to convert a value in the JavaRDD to a
   *                  HBase Increments
   * @batchSize       The number of increments to batch before sending to HBase
   */
  def bulkIncrement[T](javaRdd: JavaRDD[T], tableName: String,
      f: Function[T,Increment], batchSize:Integer) {
    hbc.bulkIncrement(javaRdd.rdd, tableName, (t:T) => f.call(t), batchSize)
  }
  
  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   *
   * It allow addition support for a user to take a JavaRDD and 
   * generate delete and send them to HBase.  
   * 
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaRDD   Original JavaRDD with data to iterate over
   * @param tableName The name of the table to delete from
   * @param f         Function to convert a value in the JavaRDD to a
   *                  HBase Deletes
   * @batchSize       The number of delete to batch before sending to HBase
   */
  def bulkDelete[T](javaRdd: JavaRDD[T], tableName: String,
      f: Function[T, Delete], batchSize:Integer) {
    hbc.bulkDelete(javaRdd.rdd, tableName, (t:T) => f.call(t), batchSize)
  }
  
  /**
   * A simple abstraction over the HBaseContext.streamBulkMutation method.
   *
   * It allow addition support for a user to take a DStream and
   * generate Increments and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaDStream Original JavaDStream with data to iterate over
   * @param tableName   The name of the table to increments into
   * @param f           Function to convert a value in the JavaDStream to a
   *                    HBase Increments
   * @batchSize         The number of increments to batch before sending to HBase
   */
  def streamBulkIncrement[T](javaDstream: JavaDStream[T],
      tableName: String,
      f: Function[T, Increment],
      batchSize: Integer) = {
    hbc.streamBulkIncrement(javaDstream.dstream, tableName,
        (t:T) => f.call(t),
        batchSize)
  }
  
  /**
   * A simple abstraction over the HBaseContext.streamBulkMutation method.
   *
   * It allow addition support for a user to take a JavaDStream and
   * generate Delete and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param JavaDStream Original DStream with data to iterate over
   * @param tableName   The name of the table to delete from
   * @param f           function to convert a value in the JavaDStream to a
   *                    HBase Delete
   */
  def streamBulkDelete[T](javaDstream: JavaDStream[T],
      tableName: String,
      f: Function[T, Delete],
      batchSize: Integer) = {
    hbc.streamBulkDelete(javaDstream.dstream, tableName,
        (t:T) => f.call(t),
        batchSize)
  }
  
  
  /**
   * A simple abstraction over the bulkCheckDelete method.
   *
   * It allow addition support for a user to take a JavaDStream and
   * generate CheckAndDelete and send them to HBase.
   *
   * The complexity of managing the HConnection is
   * removed from the developer
   *
   * @param DStream    Original JavaDStream with data to iterate over
   * @param tableName  The name of the table to delete from
   * @param f          function to convert a value in the JavaDStream to a
   *                   HBase Delete
   */
  def streamBulkCheckAndDelete[T](javaDstream: JavaDStream[T],
      tableName: String,
      f: Function[T, (Array[Byte], Array[Byte], Array[Byte], Array[Byte], Delete)]) = {
    hbc.streamBulkCheckAndDelete(javaDstream.dstream, tableName,
        (t:T) => f.call(t))
  }
  
  /**
   * A simple abstraction over the HBaseContext.mapPartition method.
   *
   * It allow addition support for a user to take a JavaRDD and generates a
   * new RDD based on Gets and the results they bring back from HBase
   *
   * @param RDD       Original JavaRDD with data to iterate over
   * @param tableName The name of the table to get from
   * @param makeGet   Function to convert a value in the JavaRDD to a
   *                  HBase Get
   * @param convertResult This will convert the HBase Result object to
   *                      what ever the user wants to put in the resulting
   *                      JavaRDD
   * return           new JavaRDD that is created by the Get to HBase
   */
  def bulkGet[T, U](tableName: String,
      batchSize:Integer,
      javaRdd: JavaRDD[T],
      makeGet: Function[T, Get],
      convertResult: Function[Result, U]): JavaRDD[U] = {
    JavaRDD.fromRDD(hbc.bulkGet(tableName,
        batchSize,
        javaRdd.rdd,
        (t:T) => makeGet.call(t),
        (r:Result) => {convertResult.call(r)}))(fakeClassTag[U])
  }
  
  /**
   * A simple abstraction over the HBaseContext.streamMap method.
   *
   * It allow addition support for a user to take a DStream and
   * generates a new DStream based on Gets and the results
   * they bring back from HBase
   *
   * @param DStream   Original DStream with data to iterate over
   * @param tableName The name of the table to get from
   * @param makeGet   Function to convert a value in the JavaDStream to a
   *                  HBase Get
   * @param convertResult This will convert the HBase Result object to
   *                      what ever the user wants to put in the resulting
   *                      JavaDStream
   * return               new JavaDStream that is created by the Get to HBase
   */
  def streamBulkGet[T, U](tableName:String,
      batchSize:Integer,
      javaDStream: JavaDStream[T],
      makeGet: Function[T, Get], 
      convertResult: Function[Result, U]) {
    JavaDStream.fromDStream(hbc.streamBulkGet(tableName,
        batchSize,
        javaDStream.dstream,
        (t:T) => makeGet.call(t),
        (r:Result) => convertResult.call(r) )(fakeClassTag[U]))(fakeClassTag[U])
  }
   
  /**
   * This function will use the native HBase TableInputFormat with the
   * given scan object to generate a new JavaRDD
   *
   *  @param tableName the name of the table to scan
   *  @param scan      the HBase scan object to use to read data from HBase
   *  @param f         function to convert a Result object from HBase into
   *                   what the user wants in the final generated JavaRDD
   *  @return          new JavaRDD with results from scan
   */
  def hbaseRDD[U](tableName: String, 
      scans: Scan,
      f: Function[(ImmutableBytesWritable, Result), U]): 
      JavaRDD[U] = {
    JavaRDD.fromRDD(
        hbc.hbaseRDD[U](tableName, 
            scans, 
            (v:(ImmutableBytesWritable, Result)) => 
              f.call(v._1, v._2))(fakeClassTag[U]))(fakeClassTag[U])
  } 
  
  /**
   * A overloaded version of HBaseContext hbaseRDD that predefines the
   * type of the outputing JavaRDD
   *
   *  @param tableName the name of the table to scan
   *  @param scan      the HBase scan object to use to read data from HBase
   *  @return New JavaRDD with results from scan
   *
   */
  def hbaseRDD(tableName: String, 
      scans: Scan): JavaRDD[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] = {
    JavaRDD.fromRDD(hbc.hbaseRDD(tableName, scans))
  }

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
   *
   * This method is used to keep ClassTags out of the external Java API, as the Java compiler
   * cannot produce them automatically. While this ClassTag-faking does please the compiler,
   * it can cause problems at runtime if the Scala API relies on ClassTags for correctness.
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior, just worse performance
   * or security issues. For instance, an Array[AnyRef] can hold any type T, but may lose primitive
   * specialization.
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]


}