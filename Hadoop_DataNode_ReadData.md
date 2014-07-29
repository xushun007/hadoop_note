# Hadoop 数据节点之读数据

徐顺 2014-07-14

## 简介
在HDFS中，文件由一个或者多个数据块组成，数据块以本地文件的形式存储在数据节点上，并对外提供文件数据的访问功能。

客户端读写文件时，先通过与名字节点交互获得数据块与数据节点的对应关系，然后进一步与数据节点进行交互
在HDFS中，名字节点和数据节点为主从架构，数据节点接收名字节点的管理，执行名字节点下达的指令。

## 流式接口

为了提高数据吞吐量，HDFS对文件的读写采用基于TCP流的数据访问接口，而不是IPC接口。

流式接口的实现是基于Java Socket、NIO和多线程，提供了一个高效率、稳定的访问接口。

## 存储节点

数据存储相关的类有StorageInfo、Storage和DataSorage。其中继承关系为：

	StorageInfo <-- Storage <-- DataSorage
                            <-- FSImage

StorageInfo、Storage和DataSorage的基本属性：

	public class StorageInfo {
	  public int   layoutVersion;  // Version read from the stored file.
	  public int   namespaceID;    // namespace id of the storage
	  public long  cTime;          // creation timestamp
	  ...
	}

	public abstract class Storage extends StorageInfo {
	  private NodeType storageType;    // Type of the node using this storage 
	  protected List<StorageDirectory> storageDirs = new ArrayList<StorageDirectory>();
	  ...
	}

	public class DataStorage extends Storage { 
	  private String storageID;
	  ...
	}

数据节点存储DataStorage继承Storage(抽象类)，FSImage同样继承Storage，用于组织名字节点的磁盘数据。
StorageInfo为数据节点和名字节点提供通用的存储服务，Storage可以管理多个存储目录StorageDirectory(内部类)

* layoutVersion：存储系统信息结构的版本号
* namespaceID：存储系统名字空间标志
* cTime：存储系统创建时间
* storageType：存储类别，在数据节点中的值为DATA_NODE
* storageID：存储ID

DataStorage用于数据节点存储空间的生存期管理，而与数据节点存储逻辑相关的操作，如创建数据块文件、校验信息文件则在FSDataset中定义。

FSDataset继承FSDatasetInterface，FSDataset接口方法大致分为两类:

* 数据块相关，如创建数据块，打开输入/输出流
* 校验文件相关，如维护数据块文件与校验文件的相关性，打开输入/输出流

FSDataset将它管理的存储空间分为3个级别：FSDir、FSVolume和FSVolumeSet。

* FSDir对象表示了"current"目录下面的子目录，其中"current"子目录保存着已经写入HDFS文件系统的数据块
* FSVolume是数据目录配置项${dfs.data.dir}中的一项，数据节点可以管理一个或者多个数据目录
* FSVolumeSet管理1个或者多个FSVolume对象

## 流式接口实现

数据节点通过数据节点存储DataStorage和文件系统数据集FSDataset，将物理存储抽象成基于两者的服务，流式接口构建在此服务上。
数据节点的流式接口包含读数据、写数据、数据块替换、数据块复制和数据校验。

### DataXceiverServer & DataXceiver

DataNode.startDataNode()中，数据节点创建ServerSocket对象并绑定到监听地址的监听端口上，并设置Socket接收的缓冲区大小，合理的缓冲区可以提高数据节点的吞吐量。

接下来，将创建流式服务线程组，创建DataXceiverServer服务器，并设置为守护线程。


    void startDataNode(Configuration conf,  AbstractList<File> dataDirs, SecureResources resources ) throws IOException {
	    ...
	    // connect to name node
	    this.namenode = (DatanodeProtocol) RPC.waitForProxy(DatanodeProtocol.class,
	                       DatanodeProtocol.versionID, nameNodeAddr,  conf);
	    // get version and id info from the name-node
	    NamespaceInfo nsInfo = handshake();

	    // find free port or use privileged port provide
	    ServerSocket ss;
	    if(secureResources == null) {
	      ss = (socketWriteTimeout > 0) ? 
	        ServerSocketChannel.open().socket() : new ServerSocket();
	      Server.bind(ss, socAddr, 0);
	    } else {
	      ss = resources.getStreamingSocket();
	    }
	    ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE); 

	    this.threadGroup = new ThreadGroup("dataXceiverServer");
	    this.dataXceiverServer = new Daemon(threadGroup, 
	        new DataXceiverServer(ss, conf, this));
	    this.threadGroup.setDaemon(true); // auto destroy when empty
	    ...
    }

在DataNode.startDataNode()中创建了DataXceiverServer对象，DataXceiverServer通过ServerSocket.accept()方法不断接收来自客户端的连接，并创建相应的DataXceiver，处理客户的请求。每一个请求对应一个DataXceiver线程，即一客户一线程模式。

DataXceiverServer与DataXceiver可可以相当于主从模式，与Memcached的线程模式类似。

	class DataXceiverServer implements Runnable, FSConstants {  
	  ServerSocket ss;
	  DataNode datanode;
	  // Record all sockets opend for data transfer
	  Map<Socket, Socket> childSockets = Collections.synchronizedMap( new HashMap<Socket, Socket>());
	  static final int MAX_XCEIVER_COUNT = 256;
	  int maxXceiverCount = MAX_XCEIVER_COUNT;  

	  ...
	  public void run() {
	    while (datanode.shouldRun) {
	      try {
	        Socket s = ss.accept();
	        s.setTcpNoDelay(true);
	        new Daemon(datanode.threadGroup,  new DataXceiver(s, datanode, this)).start();
	      } catch (AsynchronousCloseException ace) {
	          LOG.warn(datanode.dnRegistration + ":DataXceiveServer:" + StringUtils.stringifyException(ace));
	          datanode.shouldRun = false;
	      } catch (...) {
	      	...
	      }
	    }
	    try {
	      ss.close();
	    } 
	    ...
	  }
	}

* childSockets包含了所有打开的用于数据传输的Socket，这些socket被DataXceiver所用
* maxXceiverCount是数据节点流式接口能够支持的最大客户数，默认256

DataXceiverServer只处理客户端的连接请求，实际的请求处理和数据交换都通过DataXceiver处理。

	// Thread for processing incoming/outgoing data stream.
	class DataXceiver implements Runnable, FSConstants {
	  Socket s;
	  final String remoteAddress; // address of remote side
	  final String localAddress;  // local address of this daemon
	  DataNode datanode;
	  DataXceiverServer dataXceiverServer;
	  private boolean connectToDnViaHostname;
	  
	  // Read/write data from/to the DataXceiveServer.
	  public void run() {
	    DataInputStream in=null; 
	    try {
	      in = new DataInputStream( new BufferedInputStream(NetUtils.getInputStream(s), SMALL_BUFFER_SIZE));
	      short version = in.readShort();
	      if ( version != DataTransferProtocol.DATA_TRANSFER_VERSION ) {
	        throw new IOException( "Version Mismatch" );
	      }
	      boolean local = s.getInetAddress().equals(s.getLocalAddress());
	      byte op = in.readByte();
	      // Make sure the xciver count is not exceeded
	      int curXceiverCount = datanode.getXceiverCount();
	      if (curXceiverCount > dataXceiverServer.maxXceiverCount) {
	        throw new IOException("xceiverCount " + curXceiverCount
	                              + " exceeds the limit of concurrent xcievers "
	                              + dataXceiverServer.maxXceiverCount);
	      }
	      long startTime = DataNode.now();
	      switch ( op ) {
	      case DataTransferProtocol.OP_READ_BLOCK:
	        readBlock( in );
	        ...
	        break;
	      case DataTransferProtocol.OP_WRITE_BLOCK:
	        writeBlock( in );
	        datanode.myMetrics.addWriteBlockOp(DataNode.now() - startTime);
	        if (local)
	          datanode.myMetrics.incrWritesFromLocalClient();
	        else
	          datanode.myMetrics.incrWritesFromRemoteClient();
	        break;
	      case DataTransferProtocol.OP_REPLACE_BLOCK: // for balancing purpose; send to a destination
	        replaceBlock(in);
	        datanode.myMetrics.addReplaceBlockOp(DataNode.now() - startTime);
	        break;
	      case DataTransferProtocol.OP_COPY_BLOCK:
	            // for balancing purpose; send to a proxy source
	        copyBlock(in);
	        datanode.myMetrics.addCopyBlockOp(DataNode.now() - startTime);
	        break;
	      case DataTransferProtocol.OP_BLOCK_CHECKSUM: //get the checksum of a block
	        getBlockChecksum(in);
	        datanode.myMetrics.addBlockChecksumOp(DataNode.now() - startTime);
	        break;
	      default:
	        throw new IOException("Unknown opcode " + op + " in data stream");
	      }
	    } catch (Throwable t) {
	      LOG.error(datanode.dnRegistration + ":DataXceiver",t);
	    } finally {
	      LOG.debug(datanode.dnRegistration + ":Number of active connections is: "
	                               + datanode.getXceiverCount());
	      IOUtils.closeStream(in);
	      IOUtils.closeSocket(s);
	      dataXceiverServer.childSockets.remove(s);
	    }
	  }
	  ...
	}

从DataXceiver.run()可以看到，首先DataXceiver的工作流程为：

* 创建数据输入流，读取协议版本号，并进行版本检测。这些字段读取的顺序在之前介绍的协议中已经说明。
* 检查连接请求数是否超过预定的阈值，用来保证服务质量。
* 根据读取的操作码，进行响应的服务，如读数据、写数据、数据块替换、数据块复制和校验和检查等。

DataXceiverServer和DataXceiver两者协作，采用一客户/一线程模式，处理来自客户端的流式请求服务，有效的提高分布式文件系统的吞吐量。


### 读数据

读数据操作由准备阶段、数据传输阶段、数据发送阶段和速度控制等组成

在Hadoop 流式接口的读数据报文中介绍过，读数据的操作码为81。当从输入流中读取的操作码为81时，调用DataXceiver.readBlock()处理请求。

操作步骤：

* 读取协议字段，包括blockId、generationStamp、startOffset、length、clientName和accessToken
* 参数校验
* 根据请求信息构建BlockSender数据块发送器发送数据
* 清理

DataXceiver.readBlock()代码如下


	private void readBlock(DataInputStream in) throws IOException {
		long blockId = in.readLong();          
		Block block = new Block( blockId, 0 , in.readLong());

		long startOffset = in.readLong();
		long length = in.readLong();
		String clientName = Text.readString(in);
		Token<BlockTokenIdentifier> accessToken = new Token<BlockTokenIdentifier>();
		accessToken.readFields(in);
		OutputStream baseStream = NetUtils.getOutputStream(s, datanode.socketWriteTimeout);
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));

		...
		// send the block
		BlockSender blockSender = null;
		try {
		  try {
		    blockSender = new BlockSender(block, startOffset, length,
		        true, true, false, datanode, clientTraceFmt);
		  } catch(IOException e) {
		    out.writeShort(DataTransferProtocol.OP_STATUS_ERROR);
		    throw e;
		  }

		  out.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS); // send op status
		  long read = blockSender.sendBlock(out, baseStream, null); // send data

		  if (blockSender.isBlockReadFully()) {
		    try {
		      if (in.readShort() == DataTransferProtocol.OP_STATUS_CHECKSUM_OK  && 
		          datanode.blockScanner != null) {
		        datanode.blockScanner.verifiedByClient(block);
		      }
		    } catch (IOException ignored) {}
		  }
		} catch ( ... ) {
		  ...
		} finally {
		  IOUtils.closeStream(out);
		  IOUtils.closeStream(blockSender);
		}
	}

DataXceiver.readBlock()中对数据块检验中包含了一块性能优化的代码。
客户端成功读取并通过校验，会发送一个OP_STATUS_CHECKSUM_OK的操作码通知数据节点，如果数据节点发送了一个完整的数据块，则通知数据块扫描器BlockScanner将该数据块标记为校验成功，减少数据块扫描器的工作量，降低资源的消耗。

#### 数据块发送

读请求的数据发送由BlockSender负责，包括准备、发送读请求应答头、发送应答数据包和清理工作等。

    BlockSender(Block block, long startOffset, long length,
              boolean corruptChecksumOk, boolean chunkOffsetOK,
              boolean verifyChecksum, DataNode datanode, String clientTraceFmt)
      throws IOException {
    try {
      this.block = block;
      ...
      // version check & init checksum
  
      bytesPerChecksum = checksum.getBytesPerChecksum();
      if (bytesPerChecksum > 10*1024*1024 && bytesPerChecksum > blockLength){
        checksum = DataChecksum.newDataChecksum(checksum.getChecksumType(),
                                   Math.max((int)blockLength, 10*1024*1024));
        bytesPerChecksum = checksum.getBytesPerChecksum();        
      }
      checksumSize = checksum.getChecksumSize();

      if (length < 0) {
        length = blockLength;
      }

      endOffset = blockLength;
      if (startOffset < 0 || startOffset > endOffset
          || (length + startOffset) > endOffset) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match " + block + " ( blockLen " + endOffset + " )";
        LOG.warn(datanode.dnRegistration + ":sendBlock() : " + msg);
        throw new IOException(msg);
      }

      
      offset = (startOffset - (startOffset % bytesPerChecksum));
      if (length >= 0) {
        // Make sure endOffset points to end of a checksumed chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % bytesPerChecksum != 0) {
          tmpLen += (bytesPerChecksum - tmpLen % bytesPerChecksum);
        }
        if (tmpLen < endOffset) {
          endOffset = tmpLen;
        }
      }

      // seek to the right offsets
      if (offset > 0) {
        long checksumSkip = (offset / bytesPerChecksum) * checksumSize;
        // note blockInStream is  seeked when created below
        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }
      }
      seqno = 0;

      blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
      if (blockIn instanceof FileInputStream) {
        blockInFd = ((FileInputStream) blockIn).getFD();
      } else {
        blockInFd = null;
      }
      memoizedBlock = new MemoizedBlock(blockIn, blockLength, datanode.data, block);
    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      IOUtils.closeStream(blockIn);
      throw ioe;
    }
    }	

准备工作由BlockSender构造方法完成，主要包含以下几个步骤：

* 根据参数初始化成员属性
* 协议版本检查和checksum初始化
* 计算数据块的偏移位置和长度，包括offset、endOffset、length
* 打开数据块文件输入流

构造方法决定了需要从数据文件和校验文件中读取哪些数据，需考虑到块的边界问题。因为校验信息是按块组织的，为了让客户端能够进行数据校验，需要返回用户读取数据的所有块。

读请求的应答包括应答头和一个或者多个应答数据包，具体逻辑由BlockSender.sendBlock()和BlockSender.sendChunks()实现。
BlockSender.sendBlock()用来读取一个数据块的数据和校验数据发送到客户端或者数据节点。


    long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
	...
    try {
      ...
      int maxChunksPerPacket;
      int pktSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
      if (transferToAllowed && !verifyChecksum &&  baseStream instanceof SocketOutputStream && 
          blockIn instanceof FileInputStream) {
        ...
        maxChunksPerPacket = (Math.max(BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO)
                              + bytesPerChecksum - 1)/bytesPerChecksum;
        pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
      } else {
        maxChunksPerPacket = Math.max(1, (BUFFER_SIZE + bytesPerChecksum - 1)/bytesPerChecksum);
        pktSize += (bytesPerChecksum + checksumSize) * maxChunksPerPacket;
      }

      ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);

      while (endOffset > offset) {
        manageOsCache();
        long len = sendChunks(pktBuf, maxChunksPerPacket,  streamForSendChunks);
        offset += len;
        totalRead += len + ((len + bytesPerChecksum - 1)/bytesPerChecksum* checksumSize);
        seqno++;
      }
      try {
        out.writeInt(0); // mark the end of block        
        out.flush();
      } catch (IOException e) { //socket error
        throw ioeToSocketException(e);
      }
    }
    ...
    finally {
      ...
      close();
    }
    blockReadFully = (initialOffset == 0 && offset >= blockLength);
    return totalRead;
    } 

读数据的应答头包括数据校验类型、块大小和可选偏移量。BlockSender既可以用于客户端数据读也可以用于数据块复制，若为数据块复制，则不需要提供偏移量。

BlockSender.sendBlock()的主要逻辑如下：

* 根据配置计算缓冲区大小，缓冲区可一次发送多个数据块
* 循环调用BlockSender.sendChunks()发送数据
* 发送所有数据完毕后，往客户端输出流中写入0，以结束一次读操作。

应答数据包包头包括：packageLen(包长度)、offset、seqno、tail(是否最后一个应答包)和length(数据长度)


应答数据和校验数据通过BlockSend.sendBlock()方法循环调用Blocksend.sendChunks()发送。

    private int sendChunks(ByteBuffer pkt, int maxChunks, OutputStream out) 
                         throws IOException {
    // Sends multiple chunks in one packet with a single write().

    int len = (int) Math.min(endOffset - offset, (((long) bytesPerChecksum) * ((long) maxChunks)));
    if (len > bytesPerChecksum && len % bytesPerChecksum != 0) {
      len -= len % bytesPerChecksum;
    }
    if (len == 0) {
      return 0;
    }

    int numChunks = (len + bytesPerChecksum - 1)/bytesPerChecksum;
    int packetLen = len + numChunks*checksumSize + 4;
    pkt.clear();
    
    // write packet header
    pkt.putInt(packetLen);
    pkt.putLong(offset);
    pkt.putLong(seqno);
    pkt.put((byte)((offset + len >= endOffset) ? 1 : 0));
    pkt.putInt(len);
    
    int checksumOff = pkt.position();
    int checksumLen = numChunks * checksumSize;
    byte[] buf = pkt.array();
    
    if (checksumSize > 0 && checksumIn != null) {
      try {
        checksumIn.readFully(buf, checksumOff, checksumLen);
      } 
      ...
    }
    
    int dataOff = checksumOff + checksumLen;
    
    if (blockInPosition < 0) {
      //normal transfer
      IOUtils.readFully(blockIn, buf, dataOff, len);

      if (verifyChecksum) {
        int dOff = dataOff;
        int cOff = checksumOff;
        int dLeft = len;

        for (int i=0; i<numChunks; i++) {
          checksum.reset();
          int dLen = Math.min(dLeft, bytesPerChecksum);
          checksum.update(buf, dOff, dLen);
          if (!checksum.compare(buf, cOff)) {
            throw new ChecksumException("Checksum failed at " + 
                                        (offset + len - dLeft), len);
          }
          dLeft -= dLen;
          dOff += dLen;
          cOff += checksumSize;
        }
      }
      
      // only recompute checksum if we can't trust the meta data due to 
      // concurrent writes
      if (memoizedBlock.hasBlockChanged(len)) {
        ChecksumUtil.updateChunkChecksum(
          buf, checksumOff, dataOff, len, checksum
        );
      }
      
      try {
        out.write(buf, 0, dataOff + len);
      } catch (IOException e) {
        throw ioeToSocketException(e);
      }
    } else {
      try {
        //use transferTo(). Checks on out and blockIn are already done. 
        SocketOutputStream sockOut = (SocketOutputStream) out;
        FileChannel fileChannel = ((FileInputStream) blockIn).getChannel();

        if (memoizedBlock.hasBlockChanged(len)) {
          fileChannel.position(blockInPosition);
          IOUtils.readFileChannelFully( fileChannel, buf, dataOff, len );
          
          ChecksumUtil.updateChunkChecksum( buf, checksumOff, dataOff, len, checksum );          
          sockOut.write(buf, 0, dataOff + len);
        } else {
          //first write the packet
          sockOut.write(buf, 0, dataOff);
          // no need to flush. since we know out is not a buffered stream.
          sockOut.transferToFully(fileChannel, blockInPosition, len);
        }

        blockInPosition += len;

      } catch (IOException e) {
        throw ioeToSocketException(e);
      }
    }

    if (throttler != null) { // rebalancing so throttle
      throttler.throttle(packetLen);
    }

    return len;
    }


数据传输共有两种方式，一种是正常的发送，一种是通过NIO的transferTo()方法，"零拷贝"进行数据高效传输，是的数据开的数据不经过数据节点，即数据不需要经过用户空间缓冲区的中转，直接发送，缺点是不能进行数据校验。
普通数据发送的过程中，需要进行校验。

memoizedBlock.hasBlockChanged()判断数据块读写是否有竞争。

BlockTransferThrottler用于控制数据发送的速度。通过简单的计算，得到每秒能够发送的数据，然后通过时间控制行数据的发送。

