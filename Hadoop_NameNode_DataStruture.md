# Hadoop NameNode数据结构

徐顺 2014-07-22

## 数据结构

在Hadoop中，名字节点对文件进行了的抽象，主要包含Inode、INodeDirectory、INodeFile和INodeFileUnderConstruction。

继承关系：

	INode <-- INodeFile <-- INodeFileUnderConstruction
	      <-- INodeFileDirectory

INode保存文件/目录的共有属性，包括名字、父目录、权限、修改时间和访问时间。

	abstract class INode implements Comparable<byte[]>, FSInodeInfo {
	  protected byte[] name;
	  protected INodeDirectory parent;
	  protected long modificationTime;
	  protected long accessTime;
	  private long permission;
	  ...
	}

INodeDirectory抽象了HDFS中的目录，目录是一个容器，可以包含一组文件和目录。

	class INodeDirectory extends INode {
	  private List<INode> children;
	  ...
	}

INodeFile包含了文件特有的两个属性：header和blocks。header是一个长整形类型，其中高16位代表文件副本系数，低48位代表数据块的大小。

	class INodeFile extends INode {
	  protected long header;
	  protected BlockInfo blocks[] = null
	  ...
	}

	// BlocksMap.BlockInfo
	static class BlockInfo extends Block implements LightWeightGSet.LinkedElement {
	  private INodeFile          inode;
	  private Object[] triplets;
	  ...
	}

BlockInfo继承Block，保存了数据块与文件、数据块与数据节点之间的关系。
第i个数据节点信息保存在triplets[3i]中，triplets[3i+1]和triplets[3i+2]保存了该数据节点上的其他两个数据块信息。

INodeFileUnderConstruction继承INodeFile，为处于构建状态的文件索引节点，当客户端为写数据而打开文件时，该文件就处于构建状态。

	class INodeFileUnderConstruction extends INodeFile {
	  String clientName;         // lease holder
	  private final String clientMachine;
	  private final DatanodeDescriptor clientNode; // if client is a cluster node too.
	  private int primaryNodeIndex = -1; //the node working on lease recovery
	  private DatanodeDescriptor[] targets = null;   //locations for last block
	  private long lastRecoveryTime = 0;
	  ...
	}

* clientNode：客户端数据节点，当客户端运行在集群内的某一个数据节点。用于数据读写优化。
* target：最后一个数据块对应的数据流管道成员
* primaryNodeIndex和lastRecoveryTime：用于名字节点发起的数据块恢复。

BlocksMap管理名字节点上数据块的元数据，包括数据块所属的INodeFile和数据块对应的数据节点位置。如果要查找某个数据块在哪些数据节点上，可以通过BlocksMap获得。

	class BlocksMap {
	  private final int capacity;
	  private GSet<Block, BlockInfo> blocks;
	  ...
	}

其中GSet是一个特殊的集合，GSetByHashMap实现GSet接口，底层基于HashMap实现。blocks管理名字节点上的所有数据块。

DatanodeDescriptor是名字节点对数据节点的抽象，继承DatanodeInfo，增加了与名字节点相关的多个属性信息。

BlockTargetPair保存数据块与数据节点的关系。

	public class DatanodeDescriptor extends DatanodeInfo {
	  DecommissioningStatus decommissioningStatus = new DecommissioningStatus();

	  /** Block and targets pair */
	  public static class BlockTargetPair {
	    public final Block block;
	    public final DatanodeDescriptor[] targets;    

	    BlockTargetPair(Block block, DatanodeDescriptor[] targets) {
	      this.block = block;
	      this.targets = targets;
	    }
	  }

	  /** A BlockTargetPair queue. */
	  private static class BlockQueue {
	    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();
	    ...
	  }

	  private volatile BlockInfo blockList = null;
	  protected boolean isAlive = false;
	  protected boolean needKeyUpdate = false;

	  private long bandwidth;

	  /** A queue of blocks to be replicated by this datanode */
	  private BlockQueue replicateBlocks = new BlockQueue();
	  /** A queue of blocks to be recovered by this datanode */
	  private BlockQueue recoverBlocks = new BlockQueue();
	  /** A set of blocks to be invalidated by this datanode */
	  private Set<Block> invalidateBlocks = new TreeSet<Block>();
	  
	  // Set to false after processing first block report
	  private boolean firstBlockReport = true; 
	  ...
	}

* replicateBlocks：该数据节点等待复制的数据块
* recoverBlocks：该数据节点上等待租约恢复的数据块
* invalidateBlocks：该数据节点上等待删除的数据块

BlocksMap和DatanodeDescriptor一起保存名字节点的第二关系，即数据块与数据节点的映射关系。

FSNamesystem是名字节点的门面，管理名字节点的第一关系和第二关系，拥有许多管理数据块状态的属性和方法。


	public class FSNamesystem implements FSConstants, FSNamesystemMBean, FSClusterStats, 
	    NameNodeMXBean, MetricsSource {

	  volatile long pendingReplicationBlocksCount = 0L;
	  volatile long corruptReplicaBlocksCount = 0L;
	  volatile long underReplicatedBlocksCount = 0L;
	  volatile long scheduledReplicationBlocksCount = 0L;
	  volatile long excessBlocksCount = 0L;
	  volatile long pendingDeletionBlocksCount = 0L;

	  public FSDirectory dir;

	  final BlocksMap blocksMap = new BlocksMap(DEFAULT_INITIAL_MAP_CAPACITY,  DEFAULT_MAP_LOAD_FACTOR);
	  public CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();	    
	  NavigableMap<String, DatanodeDescriptor> datanodeMap = new TreeMap<String, DatanodeDescriptor>();
	  private Map<String, Collection<Block>> recentInvalidateSets = new TreeMap<String, Collection<Block>>();
	  Map<String, Collection<Block>> excessReplicateMap = new TreeMap<String, Collection<Block>>();
	  ArrayList<DatanodeDescriptor> heartbeats = new ArrayList<DatanodeDescriptor>();
	  private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
	  PendingReplicationBlocks pendingReplications;
	  public LeaseManager leaseManager = new LeaseManager(this);
	  ...
	}

* blocksMap：名字节点上的BlocksMap，维护着数据块到数据节点的映射关系
* corruptReplicas：已经损坏的数据块副本
* datanodeMap：存储标识StrorageID到DatanodeDescriptor的映射
* recentInvalidateSets：数据节点到该数据节点上等待被删除的数据块映射
* excessReplicateMap：数据节点到该数据节点上多余数据块映射
* neededReplications：准备生成复制请求的数据块副本
* pendingReplications：已经生成了复制请求的数据块副本
* leaseManager：租约管理器