from pyspark import SparkContext, SparkConf
import sys
#spark-submit /vagrant/spark.py <tablename> <tmp-dir>
if __name__ == "__main__":
		
	conf = SparkConf().setAppName('tweets spark aggregation')
	sc = SparkContext(conf=conf)
	print 'reading %s'%sys.argv[1]
	conf = {"hbase.zookeeper.quorum": "localhost", "hbase.mapreduce.inputtable": sys.argv[1]}
	keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
	valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
	hbase_rdd = sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat","org.apache.hadoop.hbase.io.ImmutableBytesWritable","org.apache.hadoop.hbase.client.Result",keyConverter=keyConv, valueConverter=valueConv, conf=conf)
	#following line doesn't work...probably due to https://issues.apache.org/jira/browse/SPARK-5361
	#hbase_rdd.saveAsHadoopFile("/usr/out.txt", outputFormatClass="org.apache.hadoop.mapred.TextOutputFormat", keyClass="org.apache.hadoop.io.Text", valueClass="org.apache.hadoop.io.Text")
	print 'writing to %s'%sys.argv[2]
	hbase_rdd.filter(lambda (x,y): len(y)>0).map(lambda (_,y):y).saveAsTextFile(sys.argv[2])