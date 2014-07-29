# Hadoop IPC原理

徐顺 2014-06-22

## 简介
Hadoop实现了一套简单、但又能精确控制通信细节的客户端存根和服务器骨架的IPC(Inter-Process Communication)机制，而没有使用Java RMI，因为有效的IPC对于Hadoop至关重要，Hadoop需要精确控制连接、超时、缓存等通信细节。

Hadoop IPC的实现基于Java动态代理、Java NIO和多线程技术。

## Hadoop IPC代码结构
Hadoop IPC代码在orgorg.apache.hadoop.ipc包中，共包含7个java文件。

* RemoteException：远程异常
* Status：枚举类，定义了远程调用的返回结果类型。包含3中状态：SUCCESS (0), ERROR (1), FATAL (-1)。
* VersionedProtocol：远程调用接口。
* ConnectionHeader：IPC客户端和服务器端连接发送的消息头。
* Client：IPC客户端逻辑，主要包含IPC连接(Client.Connection, Client.ConnectionId)和远程调用(Client.Call, Client.ParalledlCall)。
* Server：IPC服务器端逻辑，主要包含IPC连接(Server.Connection)和远程调用处理(Server.Call, Listener, Handler, Responder)。
* RPC：在Client和Server基础上实现了Hadoop IPC。



## Hadoop IPC接口
Hadoop IPC接口必须继承org.apache.hadoop.ipc.VersionedProtocol接口，VersionedProtocol()方法用于检测通信双方协议和版本的一致性。

	// 返回服务器段接口实现的版本号，用于检查通信的双方，保证使用了相同版本的接口。
	public interface VersionedProtocol {
	  public long getProtocolVersion(String protocol, 
	                                 long clientVersion) throws IOException;
	}

服务器端通过RPC.getServer()获得服务器对象，然后通过Server.start()启动服务器，停止服务器则通过Server.stop()方法。

RPC.getServer()返回一个Server实例，基于IPC接口实现对象、监听地址、端口和配置对象。其中实现对象至少实现一个IPC接口。

	public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException 

在IPC连接之前，客户端需通过RPC.getProxy()获得一个IPC客户端代理实例，当不需要时，通过RPC.stopProxy()释放资源。

RPC.getProxy()参数，protocal：IPC接口的类对象；clientVersion：接口版本；addr：服务器的Socket地址。
客户端通过RPC.getProxy()获得代理对象，通过此对象进行远程过程调用，调用会转发到服务器端执行，与普通方法的调用没有什么差别。

	public static VersionedProtocol getProxy(
	      Class<? extends VersionedProtocol> protocol,
	      long clientVersion, InetSocketAddress addr, Configuration conf)
	      throws IOException

## IPC Connection
IPC连接(Connection)是IPC客户端和服务器端通信的抽象，在远程调用之前，客户端需与服务器建立TCP连接。
由于客户端和服务器端对连接的抽象不同，因此分为Client.Connection和Server.Connection。

### ConnectionId
为提高通信效率，客户端可复用到服务器端的连接，通过ConnectionId区分。连接复用指的是相同ConnectionId的多个IPC客户端共享一个IPC连接。

	static class ConnectionId {
	     InetSocketAddress address;                   // 服务器端地址
	     UserGroupInformation ticket; 
	     Class<?> protocol;                           // IPC协议接口类             
	     private static final int PRIME = 16777619;
	     private int rpcTimeout;
	     private String serverPrincipal;
	     private int maxIdleTime; //connections will be culled if it was idle for maxIdleTime msecs
	     private final RetryPolicy connectionRetryPolicy;
	     private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
	     private int pingInterval; // how often sends ping to the server in msecs
	     ...
	}     

### ConnectionHeader
ConnectionHeader连接消息头是客户端与服务器间连接后交换的第一条消息，包含用户信息和IPC接口信息。

### Connection
Client.Connection继承Thread类，其成员变量包含三部分：TCP相关、IPC连接相关和远程调用的成员变量。

    private class Connection extends Thread {
      private InetSocketAddress server;             // server ip:port
      private ConnectionHeader header;              // connection header
      private final ConnectionId remoteId;          // connection id，用于连接复用
      
      private Socket socket = null;                 // connected socket
      private DataInputStream in;
      private DataOutputStream out;
      private int rpcTimeout;
      private int maxIdleTime; //connections will be culled if it was idle for maxIdleTime msecs
      private final RetryPolicy connectionRetryPolicy;
      private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
      private int pingInterval; // how often sends ping to the server in msecs

      // currently active calls
      private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
      private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
      private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
      ...
  }    

lastActivity记录最后一次I/O时间，若长时间没有数据交换，IPC连接将进行相应的维护操作。 

calls保存当前IPC连接上的所有远程调用。

Server.Connection成员变量也同样包含三部分：TCP相关、IPC连接相关和远程调用的成员变量，但比Client.Connection要多，如通道、缓冲区、响应队列等。

### 建立连接
connections(Hashtable<ConnectionId, Connection>)管理connectionId到Connection的映射，Client需要Connection时，若有对应的Connection，则直接复用连接，否则创建新的IPC连接。

    /** 获取连接，如果有则复用 */
    private Connection getConnection(ConnectionId remoteId, Call call)
                                   throws IOException, InterruptedException {
      ...
      Connection connection;
      do {
        synchronized (connections) {
          connection = connections.get(remoteId);
          if (connection == null) {
            connection = new Connection(remoteId);
            connections.put(remoteId, connection);
          }
        }
      } while (!connection.addCall(call));
      ...
      connection.setupIOstreams();
      return connection;
    } 

Connection.addCall()将Client.Call放入calls(private Hashtable<Integer, Call>)Map中，并通知监听器，若连接正处于关闭状态，则返回false。

    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    } 

Connection.setupIOstreams()方法建立与服务器端的IPC连接，并启动线程。

    public void run() {
      ...
      while (waitForWork()) {//wait here for work - read or close connection
        receiveResponse();
      }
      close();
      ...
    }

    private void receiveResponse() {
      ...
      try {
        int id = in.readInt();                    // try to read an id
        Call call = calls.get(id);
        int state = in.readInt();     // read call status
        if (state == Status.SUCCESS.state) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          call.setValue(value);
          calls.remove(id);
        } 
        ...
      } catch (IOException e) {
        markClosed(e);
      }
    }

服务器建立IPC连接主要由Listener和Server.Connection(注：与Client.Connection不同，Server.Connection并没有继承Thread)处理。

Listener创建ServerSocketChannel、Selector和Reader线程池，并监听指定端口。

Listener和Reader都继承Thread类，两者都通过Selector注册感兴趣的事件。

    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();
      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads);
      for (int i = 0; i < readThreads; i++) {
        Selector readSelector = Selector.open();
        Reader reader = new Reader(readSelector);
        readers[i] = reader;
        readPool.execute(reader);
      }

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    } 

Listener通过doAccept()接收客户端的连接请求，选择一个Reader线程与SocketChannel关联。
Reader通过doRead()处理读事件。

    // Reader.run()
    public void run() {
        synchronized (this) {
          while (running) {
            SelectionKey key = null;
            try {
              readSelector.select();
              while (adding) {
                this.wait(1000);
              }              
              Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
              while (iter.hasNext()) {
                key = iter.next();
                iter.remove();
                if (key.isValid()) {
                  if (key.isReadable()) {
                    doRead(key);
                  }
                }
                key = null;
              }
            } 
            ...
          }
        }
    } 


## IPC Call
在客户端和服务器端建立连接之后，客户端通过IPC接口方法就可进行远程过程调用。

和Connection类似，服务器端和客户端各自实现了相应的Call逻辑(Server.Call，Client.Call)。

Client.Call对象通过IPC连接发送到服务器之后，然后在Server端构造对应的Server.Call对象。

所有在IPC客户端实例的远程调用都会被RPC.Invoker捕获，Invoker继承import java.lang.reflect.InvocationHandler类。

RPC.Invoker.invoke()方法根据method和args构造Invocation对象，然后Client.call()将创建远程调用Client.Call对象，将此对象序列化通过IPC连接发送到服务器，然后阻塞直到接收到服务器端的响应。

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      ...
      ObjectWritable value = (ObjectWritable)client.call(new Invocation(method, args), remoteId);
      return value.get();
    }

    // Client.call()
    public Writable call(Writable param, ConnectionId remoteId) throws InterruptedException, IOException {
	    Call call = new Call(param);
	    Connection connection = getConnection(remoteId, call);
	    connection.sendParam(call);                 // send the parameter
	    synchronized (call) {
	      while (!call.done) {
	      	  ...
	          call.wait();                           // wait for the result
	      }
	      ...
	      return call.value;
	    }
    }

    // Client.Call
    private static class Call {
      private int id;                               // the client's call id
      private Writable param;                       // the parameter passed
      private Connection connection;                // connection to client
      private long timestamp;           // the time received when response is null
                                        // the time served when response is not null
      private ByteBuffer response;                      // the response for this call
      ...
    }

Hadoop IPC通过Java动态代理方式，捕获调用并将调用发送到服务器端进行处理。

## Hadoop Server
Server端处理逻辑主要由监听器(Listener)、处理器(Handler)和响应器(Responder)组成。

* Listener监听客户端的连接请求和连接建立后的数据请求，并调用服务器端连接相关方法。
  连接对象的主要工作接收远程调用请求，反序列化，然后将其仿佛阻塞队列，由Handler处理。
* Handler根据远程调用Call上下文，调用具体实现对象，然后将结果序列化，将结果放入响应队列(响应队列不为空时)。
* Responder将结果发送给客户端。



## 数据读写
TCP基于字节流，没有消息边界。IPC客户端发送请求采用显示长度的方法(固定头+数据)。
Connection.sendParam()将Client.Call消息发送到远程服务器。

    public void sendParam(Call call) {
      ...
      try {
        synchronized (this.out) {
          d = new DataOutputBuffer();
          d.writeInt(call.id);
          call.param.write(d);
          byte[] data = d.getData();
          int dataLength = d.getLength();
          out.writeInt(dataLength);      //first put the data length
          out.write(data, 0, dataLength);//write the data
          out.flush();
        }
      } catch(IOException e) {
        markClosed(e);
      } finally {
        IOUtils.closeStream(d);
      }
    } 

服务器通过Connection.readAndProcess()处理连接请求，首先读取数据长度，然后读取数据，再创建Server.Call，将其放入callQueue(LinkedBlockingQueue<Call>)调用队列中。

	public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        int count = -1;
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) 
            return count;
        }
        ...
        if (data == null) {
          dataLengthBuffer.flip();
          dataLength = dataLengthBuffer.getInt();
          if (dataLength == Client.PING_CALL_ID) {
            if(!useWrap) { //covers the !useSasl too
              dataLengthBuffer.clear();
              return 0;  //ping message
            }
          }
         ...
          data = ByteBuffer.allocate(dataLength);
        }
        count = channelRead(channel, data);
        if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          ...
          boolean isHeaderRead = headerRead;
          if (useSasl) {
            saslReadAndProcess(data.array());
          } else {
            processOneRpc(data.array()); // 调用processData()方法
          }
          data = null;
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
    }

    private void processData(byte[] buf) throws  IOException, InterruptedException {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
      int id = dis.readInt();                    // try to read an id
      Writable param = ReflectionUtils.newInstance(paramClass, conf);//read param
      param.readFields(dis);        
      Call call = new Call(id, param, this);
      callQueue.put(call);              // queue the call; maybe blocked here
      incRpcCount();  // Increment the rpc count
    }

忽略心跳消息，长度为-1。 


Listener和Handler是典型的生产者-消费者模式。processData()将待处理的远程调用放入callQueue队列中，Handler从队列中取出数据并处理。
Server.call()是一个抽象方法，具体的call逻辑由子类实现(RPC.Server.call())

	  // Handler.run()
    public void run() {
      SERVER.set(Server.this);
      ByteArrayOutputStream buf =  new ByteArrayOutputStream(INITIAL_RESP_BUF_SIZE);
      while (running) {
        try {
          final Call call = callQueue.take(); // pop the queue; maybe blocked here
          CurCall.set(call);
          ...
          try {
            if (call.connection.user == null) {
              value = call(call.connection.protocol, call.param, call.timestamp);
            } else {
              value =  call.connection.user.doAs
                  (new PrivilegedExceptionAction<Writable>() {
                     public Writable run() throws Exception {
                       return call(call.connection.protocol,  call.param, call.timestamp);
                     }
                   }
                  );
            }
          } catch (Throwable e) {
          	...
          }
            
        }
      }
    }

    // Server.call()
    public abstract Writable call(Class<?> protocol,  Writable param, long receiveTime) throws IOException;

    // RPC.Server.call()
    public Writable call(Class<?> protocol, Writable param, long receivedTime)   throws IOException {
      try {
        Invocation call = (Invocation)param;
        Method method =  protocol.getMethod(call.getMethodName(),  call.getParameterClasses());
        method.setAccessible(true);

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        ...
        return new ObjectWritable(method.getReturnType(), value);

      } catch (...) {
        ...
      }
    }
  
在RPC.Server.call()中，param将转化成RPC.Invocation对象。通过Method.invoke()调用IPC服务器实现对象instance上的对应方法，完成Hadoop远程过程调用。

Handler通过调用Responder.doRespond()将处理结果交给Responder。
一般情况下，远程过程调用由Handler线程执行被调用的过程，由于资源共享和网络通信的不确定性，Hadoop IPC服务器引入Responder发送IPC的响应结果。

Responder同样继承Thread类，通过Selector监听写(OP_WRITE)事件，然后由doAsyncWrite()方法处理。

    public void run() {
      ...
      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          ...
        } 
        ...
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      ...
      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

doAsyncWrite()对输入参数进行校验后，然后调用processResponse()方法进行写数据。

processResponse()调用Server.channelWrite()往通道尽可能写多的数据，如果当前操作没有全部写完数据，则将call重新放入队列头，等待通道再次可用时，继续写数据。

Hadoop IPC对长时间没有应答的连接进行清理，时间间隔为15(PURGE_INTERVAL = 900000ms)分钟，即若调用与当前时间差超过15分钟，将调用Connection.closeConnection()方法关闭连接。

Hadoop IPC服务器分工明确并相互配合。
Listener接受连接请求和接受数据；Handler完成实际的逻辑处理(过程调用)，并可在空闲时发送响应数据；Responder处理网络忙时的响应数据发送，并进行超时检测。

