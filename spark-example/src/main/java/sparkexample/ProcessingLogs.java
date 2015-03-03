package sparkexample;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
/**
 * hdfs dfs -mkdir -p /usr/logs
 * hdfs dfs -copyFromLocal /usr/local/lib/hadoop-2.6.0/logs/*.log /usr/logs
 * spark-submit --class sparkexample.ProcessingLogs  /vagrant/spark-example/target/spark-example-1.0-SNAPSHOT.jar
 */
public class ProcessingLogs {
    public static void main( String[] args ){
        System.out.println( "Hello World!" );
        SparkConf conf = new SparkConf().setAppName("Processing tweets in java");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://localhost:9000/usr/logs/*.log");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("Total length:"+totalLength);
    }
}
