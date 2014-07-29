# Hadoop 名字节点读写数据

徐顺 2014-07-27

## 简介
本节介绍名字节点如何实现ClientProtocol，提供文件的读写操作

## 读数据
客户端读数据需要使用到的两个主要方法为：getBlockLocations()和reportBadBlocks()。

NameNode.getBlockLocations()只是简单的调用FSNameSystem.getBlockLocations()，而真正的具体实现在FSNamesystem.getBlockLocationsInternal()方法中。

	// FSNamesystem.getBlockLocations()
	public LocatedBlocks getBlockLocations(String src, long offset, long length,
	  boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
	  throws IOException {
		... // argument check
		final LocatedBlocks ret = getBlockLocationsInternal(src, 
		    offset, length, Integer.MAX_VALUE, doAccessTime, needBlockToken);  
		...
		return ret;
	}

    private synchronized LocatedBlocks getBlockLocationsInternal(String src,
                 long offset,long length,int nrBlocksToReturn,
                 boolean doAccessTime, boolean needBlockToken) throws IOException {
	    INodeFile inode = dir.getFileINode(src);
	    if(inode == null) {
	      return null;
	    }
	    if (doAccessTime && isAccessTimeSupported()) {
	      dir.setTimes(src, inode, -1, now(), false);
	    }
	    Block[] blocks = inode.getBlocks();
	    if (blocks == null) {
	      return null;
	    }
	    if (blocks.length == 0) {
	      return inode.createLocatedBlocks(new ArrayList<LocatedBlock>(blocks.length));
	    }
	    List<LocatedBlock> results;
	    results = new ArrayList<LocatedBlock>(blocks.length);
	    ...
	}

getBlockLocationsInternal()方法的第一部分代码检查参数，更新文件的访问时间。

    private synchronized LocatedBlocks getBlockLocationsInternal(String src,
                 long offset,long length,int nrBlocksToReturn,
                 boolean doAccessTime, boolean needBlockToken) throws IOException {
	    ...
	    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
	    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
	      blkSize = blocks[curBlk].getNumBytes();
	      if (curPos + blkSize > offset) {
	        break;
	      }
	      curPos += blkSize;
	    }
	    long endOff = offset + length;
	    
	    do {
	      ...
	      if (inode.isUnderConstruction() && curBlk == blocks.length - 1
	          && blocksMap.numNodes(blocks[curBlk]) == 0) {
	        INodeFileUnderConstruction cons = (INodeFileUnderConstruction)inode;
	        machineSet = cons.getTargets();
	      } else {
	        ...
	        machineSet = new DatanodeDescriptor[numMachineSet];
	        if (numMachineSet > 0) {
	          for(Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(blocks[curBlk]); it.hasNext();) {
				...
	            if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
	              machineSet[numNodes++] = dn;
	          }
	        }
	      }
	      LocatedBlock b = new LocatedBlock(blocks[curBlk], machineSet, curPos,blockCorrupt);
	      ...
	      }
	      results.add(b); 
	      curPos += blocks[curBlk].getNumBytes();
	      curBlk++;
	    } while (curPos < endOff  && curBlk < blocks.length  && results.size() < nrBlocksToReturn);
	    
	    return inode.createLocatedBlocks(results);
	}

接下来，getBlockLocationsInternal()方法主要逻辑：

* 根据offset，计算开始的数据块
* 获得当前数据块副本的状态，包括包含数据块副本的节点数、副本损坏的节点数
* 若数据块为构建文件的最后一个数据块，则认为管道中的数据节点中的副本可用
* 否则，通过第二关系，迭代查找可用节点。
* 构造LocatedBlocks对象并返回

客户端通过调用ClientProtocol.getBlockLocations()获得数据块和数据节点的关系，通过访问数据节点的流式接口获得数据，读取文件内容。

当客户端读数据并对数据进行校验出错时，调用Client.reportBadBlocks()方法将出错的数据块及节点汇报给名字节点。

	public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
		for (int i = 0; i < blocks.length; i++) {
		  Block blk = blocks[i].getBlock();
		  DatanodeInfo[] nodes = blocks[i].getLocations();
		  for (int j = 0; j < nodes.length; j++) {
		    DatanodeInfo dn = nodes[j];
		    namesystem.markBlockAsCorrupt(blk, dn);
		  }
		}
	}

FSNamesytem.markBlockAsCorrupt()方法标记损坏的数据块。

## 写数据

名字节点处理写数据既需要处理客户端的远程方法调用，也要和数据节点的远程方法调用，与读数据不同，名字节点需要维护客户端的写文件租约。

NameNode.create()创建并打开一个文件，NameNode.append()在一个已有的文件上追加数据。

NameNode.create()方法调用层次：NameNode.create() ->	FSNamesystem.startFile() ->	FSNamesystem.startFileInternal() ->	FSDirectory.addFile()

关键的逻辑步骤：

* 参数检查，包括文件名、权限、是否允许覆盖等
* 在指定目录下插入一个新的INodeFileUnderConstruction节点，然后租约管理器中添加记录

NameNode.append()返回LocatedBlock对象，客户端通过这个对象，建立数据流管道。

具体的文件写数据是客户端与数据节点进行交互，当数据节点成功接收一个数据块时，通过ClientProtocol.blockReceived()方法向名字节点汇报。为了减少到名字节点的请求量，数据节点奖多个提交请求合并成一个请求。

	public void blockReceived(DatanodeRegistration nodeReg, 
	                        Block blocks[], String delHints[]) throws IOException {
		verifyRequest(nodeReg);
		stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
		                     +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
		for (int i = 0; i < blocks.length; i++) {
		  namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
		}
	}

数据块提交意味着对某一个数据块副本的写操作已经结束，副本的位置信息进入名字节点的第二关系。

通过ClientProtocol.create()方法创建一个新文件后，然后通过ClientProtocol.addBlock()方法添加一个新的数据块。

NameNode.addBlock()通过调用FSNamesystem.getAdditionalBlock()实现具体逻辑。

	public LocatedBlock getAdditionalBlock(String src, 
	                                     String clientName,
	                                     HashMap<Node, Node> excludedNodes
	                                     ) throws IOException {
		...
		synchronized (this) {
		  ...
		  INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
		  ...
		}
		// choose targets for the new block to be allocated.
		DatanodeDescriptor targets[] = replicator.chooseTarget(src,  replication, clientNode, excludedNodes, blockSize);
		...
		// Allocate a new block and record it in the INode. 
		synchronized (this) {
		  ...
		  checkLease(src, clientName, pathINodes[inodesLen-1]);
		  INodeFileUnderConstruction pendingFile  = (INodeFileUnderConstruction)pathINodes[inodesLen - 1];
		  ...
		  // allocate new block record block locations in INode.
		  newBlock = allocateBlock(src, pathINodes);
		  pendingFile.setTargets(targets);
		  ...
		}
		...
		LocatedBlock b = new LocatedBlock(newBlock, targets, fileLength);
		...
		return b;
	}

主要逻辑步骤：

* 参数检查
* 租约检查
* 选择管道数据节点集合
* 分配新的数据块
* 构建LocatedBlock对象并返回

FSNamesystem.allocateBlock()分配一个新的数据块，blockId是随机产生且需要唯一。

	private Block allocateBlock(String src, INode[] inodes) throws IOException {
		Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0); 
		while(isValidBlock(b)) {
		  b.setBlockId(FSNamesystem.randBlockId.nextLong());
		}
		b.setGenerationStamp(getGenerationStamp());
		b = dir.addBlock(src, inodes, b);
		NameNode.stateChangeLog.info("BLOCK* allocateBlock: " +src+ ". "+b);
		return b;
	}

新的数据块通过FSDirectory.addBlock()添加到INodeFileUnderConstruction对象中，生成的block对象也由BlocksMap管理。

	Block addBlock(String path, INode[] inodes, Block block) throws IOException {
		waitForReady();
		synchronized (rootDir) {
		  INodeFile fileNode = (INodeFile) inodes[inodes.length-1];
		  // check quota limits and updated space consumed
		  updateCount(inodes, inodes.length-1, 0,
		      fileNode.getPreferredBlockSize()*fileNode.getReplication(), true);
		  // associate the new list of blocks with this file
		  namesystem.blocksMap.addINode(block, fileNode);
		  BlockInfo blockInfo = namesystem.blocksMap.getStoredBlock(block);
		  fileNode.addBlock(blockInfo);
		  ...
		}
		return block;
	}