# Hadoop 安装

徐顺 2014-06-20

## 环境简介
* CentOS6.5
* JDK1.7
* Hadoop 1.2.1

## 安装JDK

从[Java官网](http://www.oracle.com/technetwork/java/javase/downloads/index.html)下载JDK7，解压至`/usr/local/jdk1.7.0_55`

编辑/etc/profile 在末尾添加

	export JAVA_HOME=/usr/local/jdk1.7.0_55
	export CLASSPATH=.:$JAVA_HOME/jre/lib/rt.jar:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
	export PATH=$PATH:$JAVA_HOME/bin

使修改立即生效

	$source /etc/profile

## 安装Hadoop

从[Hadoop官网](http://hadoop.apache.org/)下载Hadoop-1.2.1，解压至`~/usr/Hadoop`

修改配置文件(~/Hadoop/hadoop-1.2.1/conf/目录)

core-site.xml 文件内容修改成如下：

	<configuration>
		<property>
			<name>fs.default.name</name>
			<value>hdfs://localhost:9000</value>
		</property>
	</configuration>

mapred-site.xml 文件内容修改如下：

	<configuration>
		<property>
			<name>mapred.job.tracker</name>
			<value>localhost:9001</value>
		</property>
	</configuration>

hdfs-site.xml 文件内容修改如下：

	<configuration>
		<property>
			<name>dfs.replication</name>
			<value>1</value>
		</property>
	</configuration>

hadoop-env.sh 文件里添加JAVA_HOME路径：

	export JAVA_HOME=/usr/local/jdk1.7.0_55

安装SSH，CentOS6.5已经安装了ssh， 若没安装执行`yum install ssh rsync`

配置 ssh 免登录

	$ssh-keygen -t dsa -f ~/.ssh/id_dsa

执行这条命令生成 ssh 的公钥/私钥，执行过程中，会一些提示让输入字符，一路回车即可。

	$cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

ssh 进行远程登录的时候需要输入密码，如果用公钥/私钥方式，就不需要输入密码了。上述方式就是设置公
钥/私钥登录。
	
	$ssh localhost
 
 第一次执行上述命令，会出现一个提示，输入 yes”然后回车即可。

若`ssh localhost`仍需要密码，可执行如下命令
	
	$chmod 700 ~/.ssh 
	$chmod 600 ~/.ssh/* 


## 启动 hadoop

	$cd ~/Hadoop/hadoop-1.2.1
	$./bin/hadoop namenode -format

格式化 NameNode

	$./bin/start-all.sh"

启动所有节点，包括 NameNode, SecondaryNameNode, JobTracker, TaskTracker, DataNode。

	$jps

检查各进程是否运行，这时，应该看到有 6 个 java 虚拟机的进程，分别是 Jps, NameNode, SecondaryNameNode,
DataNode, JobTracker, TaskTracker，

## 测试 hadoop

	$cd ~/Hadoop/hadoop-1.2.1"
	$./bin/hadoop fs -put README.txt readme.txt

这条命令将 README.txt 文件复制到 Hadoop 的分布式文件系统 HDFS，重命名为 readme.txt。

	$./bin/hadoop jar hadoop-examples-1.2.1.jar wordcount readme.txt output

运行 hadoop 的 examples 的 wordcount ，测试 hadoop 的执行。这条语句用 Hadoop 自带的 examples 里的
wordcount 程序，对 readme.txt 进行处理，处理后的结果放到 HDFS 的 output 目录。

	$./bin/hadoop fs -cat output/part-r-00000
这条命令查看处理结果， part-r-00000 文件存放 wordcount程序的运行结果，cat 命令将文件内容输出到屏幕，显示
字符的统计结果。

## 源码环境

* 从[Eclipse官网](http://www.eclipse.org)下载最新版Eclipse
* 新建Hadoop Java项目，然后将`$HADOOP_HOME/src/core, $HADOOP_HOME/src/hdfs, $HADOOP_HOME/src/mapred`目录复制到项目src目录下
* 项目右击属性，配置src/core, src/hdfs, src/mapred为源目录
* 从lib目录添加jar包，还需添加ant的jar包

这样，就可以在Eclipse中阅读Hadoop源码了。
