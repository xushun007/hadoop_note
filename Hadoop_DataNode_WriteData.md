# Hadoop 数据节点之写数据

徐顺 2014-07-19

## 简介
流式接口的数据读、写是非常重要的接口。与读数据相比，写数据实现起来更加复杂。

客户端写数据的操作码为80.

### 准备

之前在Hadoop流式接口中介绍过写数据的请求报文：

     +------------------------------------------------------------------------------+
     | TRANSFER_VERSION   |    80         |      blockId     |    generationStamp   |
     +------------------------------------------------------------------------------+
     |   pipelineSize     |  isRecovery   |      client      |    hasSrcDataNode    |
     +------------------------------------------------------------------------------+
     |   srcDatanode...   |   numTargets  |  targets...      |      tocken          |   
     +------------------------------------------------------------------------------+  
     |  dataChecksum      |
     +--------------------+  

与读数据类似，当从输入流中读取的操作码为80时，调用DataXceiver.writeBlock()处理请求。

操作步骤：

* 读取协议字段，包括blockId、generationStamp、pipeline、isRecovery、client 和targets等数据
* 参数校验
* 根据请求信息构建BlockReceiver数据块接收器接收数据
* 清理

DataXceiver.writeBlock()代码如下

	private void writeBlock(DataInputStream in) throws IOException {
		// 读取协议接口数据
		replyOut = new DataOutputStream( NetUtils.getOutputStream(s, datanode.socketWriteTimeout));
		...
		short mirrorInStatus = (short)DataTransferProtocol.OP_STATUS_SUCCESS;
		try {
		  // open a block receiver and check if the block does not exist
		  blockReceiver = new BlockReceiver(block, in,  s.getRemoteSocketAddress().toString(),
		      s.getLocalSocketAddress().toString(), isRecovery, client, srcDataNode, datanode);
		  if (targets.length > 0) {
		  	...
		    try {
		      ... // 包含往下一个节点进行交互的socket和流
		      blockReceiver.writeChecksumHeader(mirrorOut);
		      ...
		    } 
		    ...
		  }
		  ...
		  blockReceiver.receiveBlock(mirrorOut, mirrorIn, replyOut,  mirrorAddr, null, targets.length);
		  ...  
		} 
		...
	}

DataXceiver.writeBlock()方法首先读取数据报文基本字段，然后构建BlockReceiver，BlockReceiver的构造函数为写数据块和校验数据块文件打开输出数据流。


	BlockReceiver(Block block, DataInputStream in, String inAddr,
	            String myAddr, boolean isRecovery, String clientName, 
	            DatanodeInfo srcDataNode, DataNode datanode) throws IOException {
	try{
	  this.block = block;
	  ... 
	  streams = datanode.data.writeToBlock(block, isRecovery, clientName == null || clientName.length() == 0);
	  this.finalized = false;
	  if (streams != null) {
	    this.out = streams.dataOut;
	    this.cout = streams.checksumOut;
	    if (out instanceof FileOutputStream) {
	      this.outFd = ((FileOutputStream) out).getFD();
	    } 
	    ...
	    this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                                 streams.checksumOut, SMALL_BUFFER_SIZE));
	    }
	  }
	} 
	...
	}

DataXceiver委托BlockReceiver.receiveBlock()方法处理写数据的数据包，然后调用DataNode.notifyNamenodeReceiveBlock()通知名字节点，最后writeBlock()将关闭上下流的输入/输出流，完成一次写数据请求。

与BlockSender类似，receiveBlock()方法将数据的接收工作主要由receivePacket()处理。receivePacket()至少读入一个请求数据包。

	void receiveBlock(
	  DataOutputStream mirrOut, // output to next datanode
	  DataInputStream mirrIn,   // input from next datanode
	  DataOutputStream replyOut,  // output to previous datanode
	  String mirrAddr, DataTransferThrottler throttlerArg,
	  int numTargets) throws IOException {
	  	...
		try {
		  // write data chunk header
		  if (!finalized) {
		    BlockMetadataHeader.writeHeader(checksumOut, checksum);
		  }
		  if (clientName.length() > 0) {
		    responder = new Daemon(datanode.threadGroup,  new PacketResponder(this, block, mirrIn, 
		                                               replyOut, numTargets, Thread.currentThread()));
		    responder.start(); // start thread to processes reponses
		  }

		  // Receive until packet length is zero.
		  while (receivePacket() > 0) {}

		  // flush the mirror out
		  if (mirrorOut != null) {
		    try {
		      mirrorOut.writeInt(0); // mark the end of the block
		      mirrorOut.flush();
		    } catch (IOException e) {
		      handleMirrorOutError(e);
		    }
		  }

		  if (responder != null) {
		    ((PacketResponder)responder.getRunnable()).close();
		  }
		  ...

		} catch (IOException ioe) {
		  ...
		} finally {
		  ...
		  responder.join();		    
		}
	}

receivePacket()首先把数据包发送到下游数据节点，通过此种方式，写数据的数据包通过管道达到所有数据节点上面。

    private int receivePacket() throws IOException {
        ...	    
	    // First write the packet to the mirror:
	    if (mirrorOut != null && !mirrorError) {
	      try {
	        mirrorOut.write(buf.array(), buf.position(), buf.remaining());
	        mirrorOut.flush();
	      } catch (IOException e) {
	        handleMirrorOutError(e);
	      }
	    }
	    ...
    }

receivePacket()后续工作将请求包的数据和数据校验和写入对应的文件，只有最后一个数据节点需要通过verifyChunks()方法对数据进行校验。当发现有错误时，需要上报名字节点。

    private int receivePacket() throws IOException {
      ...
      if (mirrorOut == null || clientName.length() == 0) {
        verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff);
      }

      try {
        if (!finalized) {
            //finally write to the disk :
            out.write(pktBuf, dataOff, len);

            ...
            partialCrc.update(pktBuf, dataOff, len);
            byte[] buf = FSOutputSummer.convertToByteStream(partialCrc, checksumSize);
            checksumOut.write(buf);
            ...
        }
      } catch (IOException iex) {
        datanode.checkDiskError(iex);
        throw iex;
      }
    }

数据块接收器引入PacketResponder线程用来接受下流数据块节点数据。BlockReceiver用来接受来自上游节点的数据。

PacketResponder线程从下游节点接受数据，并在合适的时候，向上游节点发送确认包，需满足两个条件：

* 当前数据节点已经顺利处理完该数据包
* 当前数据节点(处于管道中间时)收到下游数据节点的数据包确认

当前节点由BlockReceiver线程处理数据包，BlockerReceiver.receiver()方法每处理一个数据包，就通过PacketResponder.enqueue将对应的信息(BlockReceiver.Packet)放入ackQueue中，由PacketResponder线程不断循环处理，这是一个典型的生产者-消费者线程。

	class PacketResponder implements Runnable, FSConstants {
		private LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
		...

		public void run() {
	      ...
	      while (running && datanode.shouldRun && !lastPacketInBlock) {
	        try {
	          ...
	          try { 
	            Packet pkt = null;
	            synchronized (this) {
	              // wait for a packet to arrive
	              while (running && datanode.shouldRun && ackQueue.size() == 0) {
	                  ...
	                  wait();
	                }
	                if (!running || !datanode.shouldRun) {
	                  break;
	                }
	                pkt = ackQueue.removeFirst();
	                expected = pkt.seqno;
	                notifyAll();
	              }
	              // receive an ack if DN is not the last one in the pipeline
	              if (numTargets > 0 && !localMirrorError) {
	                // read an ack from downstream datanode
	                ack.readFields(mirrorIn);
	                seqno = ack.getSeqno();
	                ...
	              }
	              lastPacketInBlock = pkt.lastPacketInBlock;
	            } 
	            ...
	            // If this is the last packet in block, then close block
	            // file and finalize the block before responding success
	            if (lastPacketInBlock && !receiver.finalized) {
	              receiver.close();
	              final long endTime = ClientTraceLog.isInfoEnabled() ? System.nanoTime() : 0;
	              block.setNumBytes(receiver.offsetInBlock);
	              datanode.data.finalizeBlock(block);
	              datanode.myMetrics.incrBlocksWritten();
	              datanode.notifyNamenodeReceivedBlock(block, 
	                  DataNode.EMPTY_DEL_HINT);
	              ...
	            }

	            // construct my ack message
	            ...
	            PipelineAck replyAck = new PipelineAck(expected, replies);
	            // send my ack back to upstream datanode
	            replyAck.write(replyOut);
	            replyOut.flush();
	           
	        } 
	        ...
	      }
	    }
	}

PacketResponder.run()的处理分为两个部分，第一部分等待之前描述的两个条件满足，第二部分为条件满足后的处理。

通过wait()等待ackQueue中的数据，如果当前节点处于数据流管道的中间，在流mirrorIn上读取下游的确定。

如果处理的是整个写请求最后一个数据包的确认，则关闭PacketResponder所属的数据块接收器对象，设置数据块长度，使用FSDataset.finalizeBlock()方法提交数据，最后通过DataNode.notifyNamenodeReceivedBlock()通知名字节点，完成一个数据块的接收。

方法的最后向上游节点发送确认包，同时携带下游节点的确认包。


