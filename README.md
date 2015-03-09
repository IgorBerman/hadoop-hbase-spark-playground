# hadoop-intro
hadoop, hbase, spark playground. 
* Contains simple vagrant(could be used on windows) + fabric file that setups single machine cluster(needs vagrant-fabric plugin)
* Contains some examples for reading/writing from hbase/hdfs from spark

# Setup
* vagrant up - starts and provisions machine with hbase/spark/hdfs installed
* vagrant ssh
* sudo su - hadoop
* cd /vagrant/spark-example
* mvn package -DskipTests=true - builds artifact

# Executing
* cd /vagrant ; nohup python /vagrant/twitter.py -ck your-consumer-key -cs your-consumer-secret -at your-access-token -ats your-access-token-secret -t thedress -f #thedress &
  * will consume tweets with #thedress filter and save them in hbase (happybase, tweepy)
  * use https://apps.twitter.com/ to generate consumer key, secret etc.
* spark-submit --class sparkexample.ProcessingTweetsFromHbase /vagrant/spark-example/target/spark-example-1.0-SNAPSHOT.jar
  * submits spark job that reads thedress hbase table, count words and then saves it into a) hdfs file b) hbase table theDressAggregate
  * code uses scala implementation from https://github.com/cloudera-labs/SparkOnHBase of JavaHBaseContext
  * thus uses scala-maven-plugin
