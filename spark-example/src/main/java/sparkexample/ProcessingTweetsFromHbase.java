package sparkexample;

import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.cloudera.spark.hbase.JavaHBaseContext;
import com.google.common.base.Splitter;

/**
 * scall code taken from https://github.com/cloudera-labs/SparkOnHBase, I've embedded it since it works with cloudera hadoop and I work with default
 * mvn package -DskipTests=true 
 * use tweets.py to load some tweets into thedress hbase table
 * spark-submit --class sparkexample.ProcessingTweetsFromHbase /vagrant/spark-example/target/spark-example-1.0-SNAPSHOT.jar
 */
public class ProcessingTweetsFromHbase {
	private static final String THE_DRESS_AGGREGATE = "theDressAggregate";
	private static final String THE_DRESS = "thedress";
	private static final String WORDS = "words";

	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
		SparkConf conf = new SparkConf().setAppName("Processing tweets in java");

		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			Configuration hbaseReadConf = new Configuration();
			hbaseReadConf.set(TableInputFormat.INPUT_TABLE, THE_DRESS);
			JavaPairRDD<ImmutableBytesWritable, Result> rdd = sc.newAPIHadoopRDD(hbaseReadConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
			JavaRDD<NavigableMap<byte[], NavigableMap<byte[], byte[]>>> cfToCells = rdd.map(f -> f._2.getNoVersionMap());
			JavaRDD<String> texts = cfToCells.map(f -> Bytes.toString(f.get(Bytes.toBytes("general")).get(Bytes.toBytes("text"))));

			System.out.println(texts.take(10));
			JavaPairRDD<String, Integer> words = texts.flatMap(text -> Splitter.on(" ").split(text)).mapToPair(w -> new Tuple2<String, Integer>(w, 1));
			JavaPairRDD<String, Integer> wordToCount = words.reduceByKey((x, y) -> x + y);
			wordToCount = wordToCount.filter(tpl -> !tpl._1.trim().equals(""));
			System.out.println(wordToCount.take(10));

			// we can save it as regular hdfs file
			try {
				wordToCount.saveAsHadoopFile("hdfs://localhost:9000/usr/logs-aggregate"+System.currentTimeMillis(), String.class, IntWritable.class, TextOutputFormat.class);
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			
			//on the other hand we can save it directly into hbase
			Configuration hwConf = HBaseConfiguration.create();
			hwConf.addResource(new Path("/usr/local/lib/hbase/conf/core-site.xml"));
			hwConf.addResource(new Path("/usr/local/lib/hbase/conf/hbase-site.xml"));

			try (HBaseAdmin admin = new HBaseAdmin(hwConf)) {
				if (!admin.isTableAvailable(THE_DRESS_AGGREGATE)) {
					HTableDescriptor theDressAggDesc = new HTableDescriptor(TableName.valueOf(THE_DRESS_AGGREGATE));
					HColumnDescriptor wordMeta = new HColumnDescriptor(WORDS.getBytes());
					theDressAggDesc.addFamily(wordMeta);
					admin.createTable(theDressAggDesc);
				}
			}
			JavaHBaseContext hbaseContext = new JavaHBaseContext(sc, hwConf);
			JavaRDD<Tuple2<String, Integer>> jRDD = wordToCount.map((t) -> t);// from JavaPairRDD to JavaRDD...do u know better way??
			hbaseContext.bulkPut(jRDD, THE_DRESS_AGGREGATE, new PutFunction(), true);
		}

	}

	public static class PutFunction implements Function<Tuple2<String, Integer>, Put> {

		private static final long serialVersionUID = 1L;

		public Put call(Tuple2<String, Integer> v) throws Exception {
			System.out.println("Putting " + v._1 + " -> "+ v._2);
			Put put = new Put(Bytes.toBytes(v._1));

			put.add(Bytes.toBytes(WORDS), Bytes.toBytes("value"), Bytes.toBytes(String.valueOf(v._2)));
			return put;
		}
	}

}
