from	__future__	import	with_statement
from	fabric.api	import	local,	settings,	abort,	run,	cd
from	fabric.contrib.console	import	confirm
from fabric.contrib.files import exists,append,contains
from fabric.operations import *
from	fabric.api	import	*
import	os
import	StringIO
#fab	-D	-H	192.168.33.11	-u	vagrant	-i	.vagrant/machines/default/virtualbox/private_key	install_supervisor

def	provision(user="vagrant",	group="vagrant"):
	install_java8()
	install_hadoop()
	
def install_hadoop():
	'''
	http://tecadmin.net/setup-hadoop-2-4-single-node-cluster-on-linux/
	'''
	_create_hadoop_user()
	_download_hadoop()
	_configure_hadoop()
	_start_hadoop()
	
def _download_hadoop():
	if not exists("/usr/local/lib/hadoop-2.6.0"):
		with cd('/usr/local/lib'):
			if not exists("hadoop-2.6.0.tar.gz"):
				sudo("wget http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz")
			sudo("tar -xvf hadoop-2.6.0.tar.gz")
			sudo("ln -s hadoop-2.6.0 hadoop")

def _replace_file_content(fname, content):
	fcontent = StringIO.StringIO()
	fcontent.write(content)
	sudo("rm -rf %s" % fname)
	put(fcontent, fname, use_sudo=True)
	fcontent.close()

def _configure_hadoop():
	with settings(sudo_user='hadoop'):
		hadoop_settings = """
		export JAVA_HOME=/usr/lib/jvm/java-8-oracle
		export HADOOP_HOME=/usr/local/lib/hadoop
		export HADOOP_INSTALL=$HADOOP_HOME
		export HADOOP_MAPRED_HOME=$HADOOP_HOME
		export HADOOP_COMMON_HOME=$HADOOP_HOME
		export HADOOP_HDFS_HOME=$HADOOP_HOME
		export YARN_HOME=$HADOOP_HOME
		export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
		export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
		"""
		if not exists("/home/hadoop/.bashrc"):
			sudo("touch /home/hadoop/.bashrc")
		if not contains("/home/hadoop/.bashrc", "export HADOOP_HOME=/usr/local/lib/hadoop"):
			append("/home/hadoop/.bashrc", hadoop_settings, use_sudo=True)
	with cd("/usr/local/lib/hadoop/etc/hadoop"):
		core_site_xml_content= """
		<configuration>
			<property>
			  <name>fs.default.name</name>
				<value>hdfs://localhost:9000</value>
			</property>
		</configuration>
		"""
		_replace_file_content("core-site.xml", core_site_xml_content)
		
		hdfs_site_xml_content="""
		<configuration>
			<property>
			 <name>dfs.replication</name>
			 <value>1</value>
			</property>

			<property>
			  <name>dfs.name.dir</name>
				<value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>
			</property>

			<property>
			  <name>dfs.data.dir</name>
				<value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>
			</property>
		</configuration>
		"""
		_replace_file_content("hdfs-site.xml", hdfs_site_xml_content)
		
		mapred_site_xml_content = """
		<configuration>
		 <property>
		  <name>mapreduce.framework.name</name>
		   <value>yarn</value>
		 </property>
		</configuration>
		"""
		_replace_file_content("mapred-site.xml", mapred_site_xml_content)
		
		yarn_site_xml_content = """
		<configuration>
		 <property>
		  <name>yarn.nodemanager.aux-services</name>
			<value>mapreduce_shuffle</value>
		 </property>
		</configuration>
		"""
		_replace_file_content("yarn-site.xml", yarn_site_xml_content)
	with settings(sudo_user='hadoop'):
		sudo("/usr/local/lib/hadoop/bin/hdfs namenode -format -nonInteractive", warn_only=True)		
	with cd('/usr/local/lib'):
		sudo("chown hadoop -R hadoop-2.6.0")
		sudo("chmod -R u+rw hadoop-2.6.0")	

def _start_hadoop():
	with settings(sudo_user='hadoop'):
		sudo("/usr/local/lib/hadoop/sbin/start-dfs.sh", warn_only=True)
		sudo("/usr/local/lib/hadoop/sbin/start-yarn.sh", warn_only=True)
	
def _create_hadoop_user():
	user_exists = run("id -u hadoop", warn_only=True)
	if user_exists.return_code == 1:
		sudo("useradd hadoop --password hadoop -d /home/hadoop -s /bin/bash")
	if not exists("/home/hadoop/.ssh"):
		sudo("mkdir -p /home/hadoop/.ssh")
		sudo("chown -R hadoop /home/hadoop")
	with settings(sudo_user='hadoop'):
		if not exists('/home/hadoop/.ssh/id_rsa'):
			sudo('ssh-keygen -t rsa -P "" -f /home/hadoop/.ssh/id_rsa')
			sudo("cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys")
			sudo("chmod 0600 /home/hadoop/.ssh/authorized_keys")
			sudo("ssh-keyscan -H localhost >> /home/hadoop/.ssh/known_hosts")
			sudo("ssh-keyscan -H 0.0.0.0 >> /home/hadoop/.ssh/known_hosts")
		
	
def	install_java8():
	'''
	http://www.webupd8.org/2014/03/how-to-install-oracle-java-8-in-debian.html
	'''
	java_version = run('java -version',warn_only=True)
	if '1.8' not in java_version:
		print 'java 1.8 not found,  installing'
		sudo('echo	"deb	http://ppa.launchpad.net/webupd8team/java/ubuntu	trusty	main"	|	tee	/etc/apt/sources.list.d/webupd8team-java.list')
		sudo('echo	"deb-src	http://ppa.launchpad.net/webupd8team/java/ubuntu	trusty	main"	|	tee	-a	/etc/apt/sources.list.d/webupd8team-java.list')
		sudo('apt-key	adv --keyserver hkp://keyserver.ubuntu.com:80	--recv-keys	EEA14886')
		sudo('apt-get	update')
		sudo('echo	oracle-java8-installer	shared/accepted-oracle-license-v1-1	select	true	|	sudo	/usr/bin/debconf-set-selections')
		sudo('apt-get	install	-y	oracle-java8-installer')
		sudo('sudo	apt-get	-y	install	oracle-java8-set-default')