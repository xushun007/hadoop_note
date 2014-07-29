# Hadoop 远程过程调用接口-服务器间接口

徐顺 2014-07-03

## 简介
服务器间接口包括名字节点、数据节点、第二名字节点之间的交互接口，它们定义在org.apache.hadoop.hdfs.server.protocol包中。

* DatanodeProtocol：数据节点与名字节点接口，数据节点通过此接口不断向名字节点发送心跳信息，报告数据节点的状态；名字节点通过此接口往数据节点下达执行命令(DatanodeCommand)。
* InterDatanodeProtocol：数据节点间接口，数据节点之间通过此接口进行通信，维护数据块的一致性。
* NamenodeProtocol：名字节点与第二名字节点的通信接口，通过此接口，第二名字节点对命名空间镜像和编辑日志进行合并等操作。

## DatanodeProtocol
DatanodeProtocol用于数据节点与名字节点通信。

在HDFS中，名字节点维护整个文件系统的文件/目录元数据、每个文件对应的数据块和数据块在数据节点的位置信息。
其中数据块所在数据节点的位置信息，在名字节点每次重新启动都会重建，包含两个步骤：

* 数据节点初始化时，将当前存储的数据块报告给名字节点。
* 心跳报告，数据节点通过心跳信息，不断报告本地数据块的变化信息，并接受来自名字节点的指令(创建、复制、移动或者删除数据块)

数据节点与名字节点的交互主要包含握手、注册、数据块报告、心跳和指令执行等交互流程。

### 握手
数据节点启动时，首先将与名字节点进行握手，进行版本号检查，决定数据节点能否加入到HDFS集群中。

具体的API为DatanodeProtocol.versionRequest()。

	public NamespaceInfo versionRequest() throws IOException;

HDFS的版本检查非常严苛，握手时会检查名字节点和数据节点的版本号，若构建版本号不同则拒绝数据节点的加入。

### 注册
注册是数据节点能正常工作之前的必要步骤，通过DatanodeProtocol.register()方法向名字节点注册。

	public DatanodeRegistration register(DatanodeRegistration registration) throws IOException;

DatanodeRegistration继承DatanodeId，在此基础上添加了多个属性，包含了名字节点识别数据节点所需要的信息。

	public class DatanodeRegistration extends DatanodeID implements Writable {
	  public StorageInfo storageInfo;
	  public ExportedBlockKeys exportedKeys;
	  ...
	} 

	public class StorageInfo {
	  public int   layoutVersion;  // Version read from the stored file.
	  public int   namespaceID;    // namespace id of the storage
	  public long  cTime;          // creation timestamp
	  ...
	}	  

### 数据块报告
数据节点注册完成之后，调用DatanodeProtocol.blockReport()方法报告数据节点所管理的全部数据块信息，已便名字节点完成数据块与数据节点的映射关系。此方法一般只在数据节点启动时使用。后续过程中，数据节点通过心跳向名字节点汇报自己的状态。

	public DatanodeCommand blockReport(DatanodeRegistration registration, long[] blocks) throws IOException;

由于名字节点与数据节点的交互，并不会保存其回话信息，所以参数仍需要传递DatanodeRegistration对象。

使用blocks使用long数组，而没有采用Block数组，主要是为了性能上一点的优化。

### 心跳 & 命令
心跳是维护名字节点和数据节点的桥梁，通过心跳信息，名字节点可以感知到数据节点的存在，了解数据节点的状态，并根据这些状态信息，对HDFS的全局进行管理，发送指定的命令给数据节点，维护HDFS高效且稳定的运行。

DatanodeProtocol.sendHeartbeat()方法向名字节点发送心跳信息，包含了数据节点的当前运行状态。

	public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
	                                   long capacity,
	                                   long dfsUsed, long remaining,
	                                   int xmitsInProgress,
	                                   int xceiverCount) throws IOException;

该方法返回的是一个DatanodeCommand数组，是一系列名字节点指令。

	public abstract class DatanodeCommand implements Writable {
	  static class Register extends DatanodeCommand {
	    ...
	  }

	  static class Finalize extends DatanodeCommand {
	    ...
	  }

	  public static final DatanodeCommand REGISTER = new Register();
	  public static final DatanodeCommand FINALIZE = new Finalize();
	  private int action;
	  ...
	}

DatanodeCommand.Register和DatanodeCommand.Finalize分别表示让数据节点重新注册和关闭数据节点。

DatanodeCommand的action命令定义在DatanodeProtocol中。

	public interface DatanodeProtocol extends VersionedProtocol {	 
	  final static int DNA_UNKNOWN = 0;    // unknown action   
	  final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode
	  final static int DNA_INVALIDATE = 2; // invalidate blocks
	  final static int DNA_SHUTDOWN = 3;   // shutdown node
	  final static int DNA_REGISTER = 4;   // re-register
	  final static int DNA_FINALIZE = 5;   // finalize previous upgrade
	  final static int DNA_RECOVERBLOCK = 6;  // request a block recovery
	  final static int DNA_ACCESSKEYUPDATE = 7;  // update access key
	  final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // update balancer bandwidth	
	  ...
	} 

DNA_TRANSFER、DNA_INVALIDATE和DNA_RECOVERBLOCK命令编号与数据块操作相关，他们使用BlockCommand携带命令执行所需要的信息。
DNA_TRANSFER用于数据块复制，当HDFS中某一数据块的副本数小于设置值时，名字节点将需要进行数据块副本备份；当HDFS中某一数据块的副本数大于设置值时，则需要通过DNA_INVALIDATE删除多余的数据块。
DNA_RECOVERBLOCK指选择某一个数据节点为主节点，协调其他数据节点，将制定的副本恢复到一致的状态。HDFS在数据块恢复过程中采取了将各个数据块回复到它们最小长度的策略。

	public class BlockCommand extends DatanodeCommand {
	  Block blocks[];
	  DatanodeInfo targets[][];
	  ...
	}

* blocks：数据块数组，包含此次命令所涉及的数据块。
* targets：二维数组，用于数据块复制和恢复。

### 汇报数据块
DatanodeProtocol.reportBadBlocks()与数据块读相关，进行数据校验时，若发现损坏的数据块，则通过此方法向名字节点进行汇报。

	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;

HDFS在以下3中环境下对数据进行校验：

* 数据节点接收数据后，存储数据之前。
* 客户端读取数据节点数据时。 
* DataBlockSanner扫描数据块时。

数据节点通过DatanodeProtocol.blockReceive()方法向名字节点报告已经完整的接收了数据块。

	public void blockReceived(DatanodeRegistration registration,
	                        Block blocks[],
	                        String[] delHints) throws IOException;

## InterDatanodeProtocol
InterDatanodeProtocol用于数据节点之间的通信，包含3个方法。

	public interface InterDatanodeProtocol extends VersionedProtocol {

	  BlockMetaDataInfo getBlockMetaDataInfo(Block block) throws IOException;

	  BlockRecoveryInfo startBlockRecovery(Block block) throws IOException;

	  void updateBlock(Block oldblock, Block newblock, boolean finalize) throws IOException;
	}

建立IPC连接是开销比较大的操作，在正常的数据块读写过程中，HDFS采用基于流式的接口，不需要通过IPC调用。但在写操作发生故障时，需要选择一个数据节点作为主节点，协调其他节点，进行数据块恢复，这将需要InterDatanodeProtocol.startBlockRecovery()和updateBlock()方法的协作。

InterDatanodeProtocol只用于异常处理。
startBlockRecovery()用于获得参与数据块恢复的各个数据节点上此数据块的信息，返回一个BlockRecoveryInfo对象。
协调者数据节点根据返回的信息，计算数据块恢复后数据块的新长度，和名字节点交互后，然后通过updaeBlock()通知其他节点进行更新和恢复(finalize参数用于更新后，是否提交数据块)。

	public class BlockRecoveryInfo implements Writable {
	  private Block block;
	  private boolean wasRecoveredOnStartup;
	  ...
	}
