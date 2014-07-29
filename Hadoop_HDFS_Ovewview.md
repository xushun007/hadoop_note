# Hadoop 文件系统设计二

## 简介
HDFS是一个分布式文件系统，具有高容错，易扩展等特点。

## HDFS体系架构
HDFS采用主从(master/slave)体系架构。
一个HDFS集群是由一个Namenode、一个SecondaryNameNode和一定数目的Datanodes组成。Namenode是一个中心服务器，负责管理文件系统的名字空间(namespace)以及客户端对文件的访问。SecondaryNameNode用于定期合并命名空间镜像和镜像编辑日志的守护进程。集群中的Datanode一般是一个节点一个，负责管理它所在节点上的存储。HDFS暴露了文件系统的名字空间，用户能够以文件的形式在上面存储数据。从内部看，一个文件其实被分成一个或多个数据块，这些块存储在一组Datanode上。Namenode执行文件系统的名字空间操作，比如打开、关闭、重命名文件或目录。它也负责确定数据块到具体Datanode节点的映射。Datanode负责处理文件系统客户端的读写请求。在Namenode的统一调度下进行数据块的创建、删除和复制。

![](img/hdfsarchitecture.gif)	

Namenode和Datanode被设计成可以在普通的商用机器上运行。这些机器一般运行着GNU/Linux操作系统(OS)。一个典型的部署场景是一台机器上只运行一个Namenode实例，而集群中的其它机器分别运行一个Datanode实例。这种架构并不排斥在一台机器上运行多个Datanode，只不过这样的情况比较少见。

集群中单一Namenode的结构大大简化了系统的架构。Namenode是所有HDFS元数据的仲裁者和管理者，这样，用户数据永远不会流过Namenode。

## 数据块
HDFS被设计成支持大文件，适用HDFS的是那些需要处理大规模的数据集的应用。这些应用都是只写入数据一次，但却读取一次或多次，并且读取速度应能满足流式读取的需要。HDFS支持文件的“一次写入多次读取”语义。

数据块是HDFS的文件存储处理单元，一个典型的数据块大小是64MB。因而，HDFS中的文件总是按照64M被切分成不同的块，每个块尽可能地存储于不同的Datanode中。采用较大的数据块，可以减少名字节点上管理文件盒数据块关系的开销，同时对数据块进行读写时，可以有效减少建立网络连接需要的成本。

在HDFS中，为了应对磁盘、机器故障造成数据库损坏的情况，数据块会在不同机器上进行备份(默认副本数为3)，如果一个数据块遭到损坏或者丢失，则从其他机器上复制相应的副本到正常的机器上面，以保证副本数目能达到正常水平。

## 名字节点
名字节点(NameNode)是HDFS主从架构中主节点的主要进程，维护整个文件系统的文件元数据、文件与数据块之间的索引(文件对应的数据库列表)。
这些信息存在在本地文件系统：命名空间镜像(FileSytem Image)和编辑日志(Edit Log)。

命名空间镜像保存某一时刻HDFS的状态(元数据、数据块索引等)，后续对文件系统的操作，保存在编辑日志中。这两者构成了名字节点的第一关系。

第二名字节点用于定期合并命名空间镜像和编辑日志，为名字节点第一关系提供一个检查点，避免编辑日志过大，导致名字节点启动过慢等问题。

## 数据节点
HDFS集群中的从节点通常都会驻留一个数据节点的守护进程，进行分布式数据的存储和读取。
HDFS将文件分块存储，这些文件块以普通的Linux文件存在。

客户端进行文件读写时，首先通过与名字节点交互告知对应的文件所在的数据块在哪些节点上，然后客户端直接与数据节点守护进程进行通信，处理对应的数据块文件，在此过程中，数据节点之间也可能进行通信，以保证数据库的冗余性。
数据节点作为从节点，通过心跳不断向名字节点汇报当前的状态，名字节点根据数据节点的反馈，进行数据集群的管理。

## 客户端
客户端是用户与HDFS进行交互的手段，HDFS提供多种客户端，包括命令行接口、Java API和C库等。

创建目录指令：

	$ hadoop fs -mkdir foodir

DistributedFileSystem继承FileSystem，实现了Hadoop文件系统接口，提供了处理HDFS文件/目录相关事务和读写数据流。
HDFS提供的抽象接口屏蔽了访问HDFS底层的各种细节，用户通过标准的Hadoop文件接口就可以访问复杂的HDFS，而不需要考虑客户端与名字节点、名字节点与数据节点、客户端与数据节点、数据节点之间交互的细节，降低应用开发难度。

## DataNodeID

	// DatanodeID 是数据节点的组成部分。
	public class DatanodeID implements WritableComparable<DatanodeID> {
	  public String name;       // hostname:port (data transfer port)
	  public String storageID;  // unique per cluster storageID
	  protected int infoPort;   // info server port
	  public int ipcPort;       // ipc server port
	  ...
	}

FSConstants接口定义了一些通用的常量。


	// The Client transfers data to/from datanode using a streaming protocol.
	public interface DataTransferProtocol {
	  public static final byte OP_WRITE_BLOCK = (byte) 80;
	  public static final byte OP_READ_BLOCK = (byte) 81;
	  public static final byte OP_REPLACE_BLOCK = (byte) 83;
	  public static final byte OP_COPY_BLOCK = (byte) 84;
	  public static final byte OP_BLOCK_CHECKSUM = (byte) 85;
	  ...
	  /** reply **/
	  public static class PipelineAck implements Writable {
	    private long seqno;          // 序列号
	    private short replies[];     // 返回状态
	    ...
	    public boolean isSuccess() {
	      for (short reply : replies) {
	        if (reply != OP_STATUS_SUCCESS) {
	          return false;
	        }
	      }
	      return true;
	    }
	}
