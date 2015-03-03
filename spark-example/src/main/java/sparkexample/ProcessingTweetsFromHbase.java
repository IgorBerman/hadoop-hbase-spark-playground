package sparkexample;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
/**
 * mvn package -DskipTests=true
 * use tweets.py to load some tweets into thedress hbase table
 * spark-submit --class sparkexample.ProcessingTweetsFromHbase  /vagrant/spark-example/target/spark-example-1.0-SNAPSHOT.jar
 */
public class ProcessingTweetsFromHbase {
    public static void main( String[] args ){
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf().setAppName("Processing tweets in java");
        
        try(JavaSparkContext sc = new JavaSparkContext(conf)){
	        Configuration hconf = new Configuration(); 
	        hconf.set(TableInputFormat.INPUT_TABLE, "thedress");
	        JavaPairRDD<ImmutableBytesWritable,Result> rdd = sc.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
	        JavaRDD<NavigableMap<byte[], NavigableMap<byte[], byte[]>>> cfToCells = rdd.map(f -> f._2.getNoVersionMap());
	        JavaRDD<String> texts = cfToCells.map(f -> Bytes.toString(f.get(Bytes.toBytes("general")).get(Bytes.toBytes("text"))));
	        Integer totalLength = texts.map(text -> text.length()).reduce((a,b) -> a+b);
	        System.out.println(totalLength);
        }
    }
    
}
