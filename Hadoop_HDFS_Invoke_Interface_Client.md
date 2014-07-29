# Hadoop 远程过程调用接口 —— 客户端接口

徐顺 2014-07-06

## 简介 
良好的接口设计可以减少各个模块的依赖性，降低编程复杂度且易于维护和扩展。
HDFS体系结构中包含三种角色：客户端、名字节点和数据节点。这三者之间有两种通信接口。

* Hadoop远程过程调用接口。
* 基于TCP/HTTP协议的流式接口。

Hadoop远程过程调用接口可分为三类：客户端相关、服务器相关和安全相关，本节介绍客户端接口。

## 客户端相关接口
* ClientProtocol：客户端与名字节点接口，是访问HDFS文件系统的入口，通过它可获取文件的元数据信息和文件对应的数据块所在的数据节点。
* ClientDatanodeProtocol：客户端与数据节点接口。

org.apache.hadoop.hdfs.protocol包含3类：数据块相关、数据节点相关和文件元数据相关。

### 数据块相关
HDFS将文件按数据块存储，每个数据块保存多个副本，在HDFS中，数据块被抽象为org.apache.hadoop.hdfs.protocol.Block。

	public class Block implements Writable, Comparable<Block> {
	  private long blockId;
	  private long numBytes;
	  private long generationStamp;
	  ...
	} 

* blockId: 	数据块唯一标识。
* numBytes：数据块大小。
* generationStamp：数据块版本号。

数据块更新后，generationStamp随之改变。数据块id决定存储在本地文件系统的数据块名字，若blockId= ${bid}，则数据块的文件名为blk_${bid}。

客户端与数据节点通信，除了需要了解数据块的基本信息外，还需要知道这些数据块存在在哪些数据节点上和数据块在文件的偏移量等信息。
org.apache.hadoop.hdfs.protocol.LocatedBlock则包含了这些信息。

	public class LocatedBlock implements Writable {
	  private Block b;
	  private long offset;  // offset of the first byte of the block in the file
	  private DatanodeInfo[] locs; 
	  private boolean corrupt;
	  private Token<BlockTokenIdentifier> blockToken = new Token<BlockTokenIdentifier>();
	  ...
	}

* locs：包含了所有可用的数据块对应的数据节点的位置，数据节点位置按照利于与客户端通信而优先排列。
* corrupt：如果所有的数据块都损坏了，则为true，否则为false。

LocatedBlocks则可一次定位多个数据块，包含一系列LocatedBlock。

	public class LocatedBlocks implements Writable {
	  private long fileLength;
	  private List<LocatedBlock> blocks; // array of blocks with prioritized locations
	  private boolean underConstruction;
	  ...
	} 

* fileLength：文件长度
* blocks：多个相关的数据块，一次获得文件中的多个数据块(不一定全部)，可以减少与名字节点的通信。
* underConstruction：文件是否处于构建状态，即是否正在写文件。

BlockLocalPathInfo用于HDFS读本地数据节点的读优化，若客户端要读取的数据恰好处理本地节点上，则直接读取本地文件，减少数据节点的负载。

	public class BlockLocalPathInfo implements Writable {
	  private Block block;
	  private String localBlockPath = "";  // local file storing the data
	  private String localMetaPath = "";   // local file storing the checksum
	  ...
	}

### 数据节点相关

DatanodeId用于标识HDFS集群的一个数据节点。

	public class DatanodeID implements WritableComparable<DatanodeID> {
	  public String name;       // hostname:port (data transfer port)
	  public String storageID;  // unique per cluster storageID
	  protected int infoPort;   // info server port
	  public int ipcPort;       // ipc server port
	  ...
	} 

* storageID：HDFS集群中数据节点存储标识。
* infoPort：数据节点HTTP/HTTPS监听端口。
* ipcPort：数据节点IPC监听端口。

DatanodeInfo继承DatanodeId, 用于数据节点间、数据节点和客户端的通信，添加了多个属性。

	public class DatanodeInfo extends DatanodeID implements Node {
	  protected long capacity;   
	  protected long dfsUsed;    
	  protected long remaining;  
	  protected long lastUpdate;  
	  protected int xceiverCount; 
	  protected String location = NetworkTopology.DEFAULT_RACK; 
	  public enum AdminStates {NORMAL, DECOMMISSION_INPROGRESS, DECOMMISSIONED; }
	  protected AdminStates adminState; 
	  ...
	} 

* capacity：数据节点容量
* dfsUsed：已使用容量
* remaining：剩余容量
* lastUpdate：状态更新时间
* xceiverCount：流接口服务线程数
* location：数据节点所处集群中位置 	
* adminState：节点状态

根据Datanode的这些状态信息，Nodenode可以分析Datanode的负载状态，用于资源调度、数据节点服务选择。

### 文件元数据相关

HdfsFileStatus和FileStatus的属性几乎一致，保存在HDFS文件/目录的相关属性。DirectoryListing用于一次返回一个目录下的多个文件/子目录属性。

	public class DirectoryListing implements Writable {
	  private HdfsFileStatus[] partialListing;
	  private int remainingEntries;
	  ...
	} 


## ClientProtocol
客户端和名字节点的通信协议在org.apache.hadoop.hdfs.protocol.ClientProtocol中定义，主要包含以下操作：

* 文件/目录元数据操作
* 读文件操作
* 写文件操作
* 状态查询与设置操作

上述操作大都与Hadoop文件系统的相关操作对应。

### 操作接口
ClientProtocol协议操作文件/目录元数据相关的主要方法有：

* 创建/追加文件：create(), append()
* 读文件：getBlockLocations(), reportBadBlocks()
* 写文件：addBlock(), abandonBlock()
* 关闭文件：fsync(), complete()
* 文件属性：getFileInfo(), getContentSummary()
* 删除：delete()
* 重命名：rename()
* 创建目录：mkdirs()
* ...

### 主要方法解析

#### ClientProtocol.create()
ClientProtocol.create()方法用于在HDFS文件系统的名字空间里中创建一个新的空文件，创建后，此文件对其他用户可见并可读，但其他用户不能对其进行重命名或者删除操作，直到租约过期。

	public void create(String src, FsPermission masked, String clientName, boolean overwrite, 
	                   boolean createParent, short replication, long blockSize ) throws IOException;

* src：文件全路径，HDFS中没有当前路径的概念
* clientName：客户端名字
* marked：文件权限。
* overrite：是否覆盖已经存在的文件
* createParent：若父目录不存在，是否递归创建
* replication：文件块副本数
* blockSize：数据块的大小

### ClientProtocol.append()
ClientProtocol.append()方法在已有的文件上追加数据，相比create()方法比较简单，它返回的是一个LocatedBlock对象，通过该对象，客户端可知往哪个数据块的什么位置开始写数据。

	public LocatedBlock append(String src, String clientName) throws IOException;

#### ClientProtocol.addBlock()
ClientProtocol.addBlock()方法用于为当前打开的文件添加一个数据块。当用于新创建一个文件后或者写满一个数据块后调用此方法，为文件分配一个新的数据块。

	public LocatedBlock addBlock(String src, String clientName) throws IOException;

#### ClientProtocol.abandonBlock()
ClientProtocol.abandonBlock()方法用于放弃一个数据块。

	public void abandonBlock(Block b, String src, String holder) throws IOException;

#### ClientProtocol.renewLease()
在名字节点中，租约记录了客户端为写数据而打开的文件信息，也就是名字节点将写特定文件的权限授权于客户端，客户端通过Client.renewLease()方法不断的定时更新租约，以持续用于对此文件的写权限。

调用ClientProtocol.renewLease()相当于往名字节点发送心跳信息。如果名字节点长时间没接收到租约更新，则认为客户端已经崩溃，将关闭打开的文件，以防止资源泄露。

	public void renewLease(String clientName) throws IOException;


### ClientProtocol.getBlockLocations()
ClientProtocol.getBlockLocations()方法根据文件名、偏移量和所有读取的文件长度，一次返回多个数据块信息，可有效的减少客户端与名字节点的通信，降低名字节点的负载。

	public LocatedBlocks  getBlockLocations(String src, long offset, long length) throws IOException;

### ClientProtocol.reportBadBlocks()
ClientProtocol.reportBadBlocks()方法用于客户端发现从数据节点传输过来的数据块受到了损坏，向名字节点报告损坏的数据块和相应的数据节点位置，可以一次报告多个损坏的数据块。

	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;


## ClientDatanodeProtocol

与ClientProtocol相比，ClientDatanodeProtocol很简单，只包含3个方法。

	public interface ClientDatanodeProtocol extends VersionedProtocol {

	  LocatedBlock recoverBlock(Block block, boolean keepLength,
	                            DatanodeInfo[] targets) throws IOException;

	  Block getBlockInfo(Block block) throws IOException;

	  BlockLocalPathInfo getBlockLocalPathInfo(Block block, 
	  	                                       Token<BlockTokenIdentifier> token) throws IOException;           
	}

* recoverBlock()用于DFSClient的输出流DFSOutputStream中，客户端向数据节点写数据过程中，可能出现错误，则客户端尝试进行数据块恢复。
* getBlockInfo()和HDFS的一致性模型相关。
* getBlockLocalPathInfo()用于本地读优化。