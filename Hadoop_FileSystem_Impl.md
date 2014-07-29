# Hadoop FileSystem & ChecksumFileSystem

徐顺 2014-06-30

## 简介
为了提供对不同数据访问的一致接口，Hadoop引入了抽象文件系统，并提供了大量的具体文件系统的实现。
Hadoop文件系统抽象类：org.apache.hadoop.fs.FileSystem。

Hadoop I/O 流参考了Java I/O流的设计思想，Java I/O是一个典型基于流的库，流代表有能力产生数据的数据源或者接收数据的接收端，其源和端可以是文件、网络套接字、声卡等。
Java的I/O库总体设计是基于装饰者模式(Decorator)跟适配器模式(Adapter)。
HDFS的文件数据I/O同样基于流。

Hadoop最常用的Java I/O流为DataInputStream和DataOutputStream，它们支持写入和读取所有Java基本类型的方法。

Hadoop抽象文件系统的方法分为两部分：

* 处理文件和目录的相关事务
* 读写文件数据

## FileSystem 接口

### fs.FileSystem

FileSystem抽象类主要包含一下几类接口：

* 打开或创建文件：FileSystem.open(), FileSystem.create(), FileSystem.append()
* 读取文件流数据：FSDataInputStream.read()
* 写文件流数据：FSDataOutputStream.write()
* 关闭文件：FSDataInputStream.close(), FSDataOutputStream.close()
* 删除文件：FileSystem.delete()
* 文件重命名：FileSystem.rename()
* 创建目录：FileSystem.mkdirs()
* 定位文件流位置：FSDataInputStream.seek()
* 获取目录/文件属性：FileSystem.getFileStatus(), FileSystem.get*()
* 设置目录/文件属性：FileSystem.set*()
* 设置/获取当前目录：FileSystem.getWorkingDirectory(), FileSystem.setWorkingDirectory()
* 获取具体的文件系统：FileSystem.get(), FileSystem.getLocal()

FileSystem.get()为工厂模式实现，用于创建多种文件系统产品。

其中上面有很多方法是抽象方法，具体的文件系统需要实现这些抽象方法。

### FileStatus
Hadoop 通过FileSystem.getFileStatus()可获得文件/目录的属性，这些属性封装在FileStatus中。
FileStatus返回给客户端关于文件的元数据信息，包含路径，长度、修改时间、访问时间等基本信息和分布式文件系统特有的副本数。

	// Interface that represents the client side information for a file.
	public class FileStatus implements Writable, Comparable {
	  private Path path;
	  private long length;
	  private boolean isdir;
	  private short block_replication;
	  private long blocksize;
	  private long modification_time;
	  private long access_time;
	  private FsPermission permission;
	  private String owner;
	  private String group;
	  ...
	}

FileStatus实现了Writable接口，因此FileStatus对象可序列化后在网络上传输。
FileStatus几乎包含了文件/目录的所有属性，这样设计的好处可以减少在分布式系统中进行网络传输的次数。

### FSDataInputStream/FSDataOutputStream
Hadoop基于流机制进行文件读写。通过FileSystem.open()可创建FSDataInputStream；通过FileSystem.create()/append()可创建FSDataOutputStream。

FSDataInputStream实现了Seekable接口和PositionedReadable接口
FSDataInputStream是装饰器模式的典型运用，实现Seekable接口和PositionedReadable接口借助其装饰的InputStream对象。

	  public class FSDataInputStream extends DataInputStream 
       implements Seekable, PositionedReadable, Closeable, HasFileDescriptor {
        public FSDataInputStream(InputStream in) throws IOException {
       	  super(in);
          if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
            throw new IllegalArgumentException(  "In is not an instance of Seekable or PositionedReadable");
          }
        }
  
        public synchronized void seek(long desired) throws IOException {
          ((Seekable)in).seek(desired);
        }
    
        public void readFully(long position, byte[] buffer)
          throws IOException {
          ((PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
        }
        ...
    }	

Seekable接口提供了在流中进行随机存取的方法，可在流中随机定位位置，然后读取输入流。
seekToNewSource()重新选择一个副本。

    public interface Seekable {
  	  // Seek to the given offset from the start of the file.
  	  void seek(long pos) throws IOException;
  	  
  	  // Return the current offset from the start of the file
  	  long getPos() throws IOException;

  	  // Seeks a different copy of the data.  Returns true if found a new source, false otherwise.
  	  boolean seekToNewSource(long targetPos) throws IOException;
	  }

PositionedReadable接口提供了从输入流中某个位置读取数据的方法，这些方法读取数据后并不改变流的当前位置。
read()和readFully()方法都是线程安全的，区别在于：前者试图读取指定长度的数据，后者读取制定长度的数据，直到读满缓冲区或者流结束。

	public interface PositionedReadable {
	  public int read(long position, byte[] buffer, int offset, int length) throws IOException;

	  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException;
	 
	  public void readFully(long position, byte[] buffer) throws IOException;
	}

FSInputStream抽象类继承InputStream，并实现PositionedReadable接口。FSInputStream拥有多个子类，具体的文件系统实现相应的输入流。

FSDataOutputStream继承DataOutputStream，Hadoop文件系统不支持随机写，因而没有实现Seekable接口。
FSDataOutputStream实现了Syncable接口，Syncable.sync()将流中的数据同步至设备中。

    public class FSDataOutputStream extends DataOutputStream implements Syncable {...}


## Hadoop 具体文件系统
Hadoop提供大量具体的文件系统实现，以满足用户访问各种数据需求。
这些文件系统直接或者间接的继承org.apache.hadoop.fs.FileSystem。

其中FilterFileSystem类似于java.io.FilterInputStream，用于在已有的文件系统之上提供新的功能，同样是包装器设计模式的运用。
ChecksumFileSystem用于在原始文件系统之上提供校验功能。

继承关系为：

    FileSystem <-- FilterFileSystem <-- ChecksumFileSystem <-- LocalFileSystem
                                                           <-- ChecksumDistributeFileSystem

## ChecksumFileSystem
Hadoop是针对大型分布式文件系统的处理，考虑到硬件/软件错误是常态而不是异常，因此需要对数据差错等进行容错和检测，以保证数据的完整性，因此错误检测和快速、自动的恢复是HDFS最核心的架构目标。

ChecksumFileSystem继承FilterFileSystem，基于CRC-32提供对文件系统的数据校验。
与其他文件系统一样，ChecksumFileSystem需要提供处理文件/目录相关事务和文件读写服务。

### 文件/目录相关事务
这部分逻辑主要保持数据文件和CRC-32校验信息文件的一致性，如数据文件重命名，则校验文件也需要重命名。
如果数据文件为：foo.txt，则校验文件为：.foo.txt.crc

以ChecksumFileSystem.delete()方法删除文件文件为例。若文件为目录则递归删除(recursive=true)；若为普通文件，则删除对应的校验文件(若存在)。

    public boolean delete(Path f, boolean recursive) throws IOException{
      FileStatus fstatus = null;
      try {
        fstatus = fs.getFileStatus(f);
      } catch(FileNotFoundException e) {
        return false;
      }
      if(fstatus.isDir()) {
        return fs.delete(f, recursive);
      } else {
        Path checkFile = getChecksumFile(f);
        if (fs.exists(checkFile)) {
          fs.delete(checkFile, true);
        }
        return fs.delete(f, true);
      }
    }


### 读文件
Hadoop读文件时，需要从数据文件和校验文件中分别读出内容，并根据校验信息对读入的数据文件内容进行校验，以判断文件的完整性。
注：若校验事变，ChecksumFileSystem无法确定是数据文件出错还是校验文件出错。

读数据流程与ChecksumFSInputChecker和其父类FSInputChecker相关。
FSInputChecker的成员变量包含数据缓冲区、校验和缓冲区和读取位置等变量。

    abstract public class FSInputChecker extends FSInputStream {
      protected Path file;        // The file name from which data is read from 
      private Checksum sum;    
      private boolean verifyChecksum = true;
      private byte[] buf;                        // 数据缓冲区
      private byte[] checksum;                   // 校验和缓冲区
      private int pos;
      private int count;
      private int numOfRetries;                  // 出错重试次数
      private long chunkPos = 0;  // cached file position
      ...
    }

ChecksumFSInputChecker构造方法对基类FSInputChecker的成员进行初始化，基于CRC-32校验，校验和大小为4字节。
对校验文件首先要进行版本校验，即文件头部是否匹配魔数"crc\0"

    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      ...
      try {
        ...
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new IOException("Not a checksum file: "+sumFile);
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, new PureJavaCrc32(), bytesPerSum, 4);
      } catch (...) {         // ignore
        set(fs.verifyChecksum, null, 1, 0);
      }
    }


FSInputChecker.read()循环调用read1()方法直到读取len个字节或者没有数据可读，返回读取的字节数。

    public synchronized int read(byte[] b, int off, int len) throws IOException {
      ... // 参数校验
      int n = 0;
      for (;;) {
        int nread = read1(b, off + n, len - n);
        if (nread <= 0) 
          return (n == 0) ? nread : n;
        n += nread;
        if (n >= len)
          return n;
      }
    }

FSInputChecker.read1()方法为了提高效率，减少内存复制的次数，若当前FSInputChecker.buf没有数据可读且要读取的len字节数大于或等于数据块大小(buf.length，默认512字节)，则通过readchecksumChunk()方法将数据直接读取目标数组中，而不需经过FSInputChecker.buf的中转。
若buf没有数据可读且读取的len字节数小于数据块大小，则通过fill()方法从数据流中一次读取一个数据块。

    private int read1(byte b[], int off, int len) throws IOException {
      int avail = count-pos;
      if( avail <= 0 ) {
        if(len>=buf.length) {
          int nread = readChecksumChunk(b, off, len);   // read a chunk to user buffer directly; avoid one copy
          return nread;
        } else {
          fill();  // read a chunk into the local buffer
          if( count <= 0 ) {
            return -1;
          } else {
            avail = count;
          }
        }
      }
      // copy content of the local buffer to the user buffer
      int cnt = (avail < len) ? avail : len;
      System.arraycopy(buf, pos, b, off, cnt);
      pos += cnt;
      return cnt;    
    }    

FSInputChecker.readChecksumChunk()方法通常需要对读取的字节序列进行校验(默认为true)，若校验不通过，可选择新的副本进行重读，如果进行了retriesLeft次重读仍然不能校验通过，则抛出异常。
readChunk()方法是一个抽象方法，FSInputChecker的子类实现它，以定义实际读取数据的逻辑。

    private int readChecksumChunk(byte b[], int off, int len)  throws IOException {
      // invalidate buffer
      count = pos = 0;
      int read = 0;
      boolean retry = true;
      int retriesLeft = numOfRetries; 
      do {
        retriesLeft--;
        try {
          read = readChunk(chunkPos, b, off, len, checksum);
          if( read > 0 ) {
            if( needChecksum() ) {
              sum.update(b, off, read);
              verifySum(chunkPos);
            }
            chunkPos += read;
          } 
          retry = false;
        } catch (ChecksumException ce) {
            if (retriesLeft == 0) {
              throw ce;
            }
            if (seekToNewSource(chunkPos)) { // 重试一个新的数据副本
              seek(chunkPos);
            } else {
              throw ce;
            }
          }
      } while (retry);
      return read;
    }

ChecksumFileSystem.ChecksumFSInputChecker实现了readChunk()的逻辑。
readChunk()它读取数据块和校验数据和，不进行两者的校验。
getChecksumFilePos()方法定位到校验和文件中pos位置对应块的边界，以便读取一个数据块对应的完整校验和。

    // ChecksumFSInputChecker.readChunk()
    protected int readChunk(long pos, byte[] buf, int offset, int len,
        byte[] checksum) throws IOException {
      boolean eof = false;
      if(needChecksum()) {
        try {
          long checksumPos = getChecksumFilePos(pos); 
          if(checksumPos != sums.getPos()) {
            sums.seek(checksumPos);
          }
          sums.readFully(checksum);
        } catch (EOFException e) {
          eof = true;
        }
        len = bytesPerSum;
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      int nread = readFully(datas, buf, offset, len);
      if( eof && nread > 0) {
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }

## 写文件
与文件/目录元数据信息的维护和读文件相比，写文件相对起来比较复杂，ChecksumFileSystem需要维护字节流上的数据读写和基于块的校验和关系。
一般而言，每{io.bytes.per.checksum}(默认512)个数据字节对应一个单独的校验和，CRC-32校验和的输出为4个字节。因此校验数据所带来的存储开销小于1%。

ChecksumFSOutputSummer继承FSOutputSummer，在基本的具体文件系统的输出流上，添加数据文件和校验文件流的输出。
继承关系：OutputStream <-- FSOutputSummer <-- ChecksumFSOutputSummer

FSOutputSummer是一个生成校验和的通用输出流，包含4个成员变量。

    abstract public class FSOutputSummer extends OutputStream {
      private Checksum sum;      // data checksum 计算校验和
      private byte buf[]; // internal buffer for storing data before it is checksumed 输出数据缓冲区
      private byte checksum[]; // internal buffer for storing checksum 校验和缓冲区
      private int count; // The number of valid bytes in the buffer. 已使用空间计数
      ...
    }

FSOutputSummer逻辑非常清晰，根据提供的字节数组，每{io.bytes.per.checksum}求出一个校验和，并根据子类所实现的writeChunk()方法写出到响应的输出流中，在ChecksumFSOutputSummer中，则分别写入文件数据流和校验文件数据流。

    // ChecksumFileSystem.CheckSumFSOutputSummer 
    private static class ChecksumFSOutputSummer extends FSOutputSummer {
      private FSDataOutputStream datas;    
      private FSDataOutputStream sums;

      ...
      @Override
      protected void writeChunk(byte[] b, int offset, int len, byte[] checksum) throws IOException {
        datas.write(b, offset, len);
        sums.write(checksum);
      }
    }

FSOutputSummer.write()方法循环调用write1()方法进行校验和计算和数据流输出。当buf的count数等于buf.length，则将数据和校验和输出到对应的流中。

    public synchronized void write(byte b[], int off, int len) throws IOException {
      ... //参数校验
      for (int n=0;n<len;n+=write1(b, off+n, len-n)) {  }
    }
   
    private int write1(byte b[], int off, int len) throws IOException {
      if(count==0 && len>=buf.length) {
        final int length = buf.length;
        sum.update(b, off, length);
        writeChecksumChunk(b, off, length, false);
        return length;
      }
      
      // copy user data to local buffer
      int bytesToCopy = buf.length-count;
      bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
      sum.update(b, off, bytesToCopy);
      System.arraycopy(b, off, buf, count, bytesToCopy);
      count += bytesToCopy;
      if (count == buf.length) { // local buffer is full
        flushBuffer();  
      } 
      return bytesToCopy;
    }

    private void writeChecksumChunk(byte b[], int off, int len, boolean keep) throws IOException {
      int tempChecksum = (int)sum.getValue();
      if (!keep) {
        sum.reset();
      }
      int2byte(tempChecksum, checksum); // 整数转字节数组
      writeChunk(b, off, len, checksum);
    }

write1()方法是用了一个实用的技巧，若当前缓冲区的写入字节数为0(count=0)且需要写入的字节数据长度大于或等于块(buf.length)的长度，则直接进行校验和计算，避免将数据拷贝到缓冲区，然后再计算校验和，减少内存拷贝的次数。
write1()方法尽可能的写入多的数据，但一次最多写入一个块。

ChecksumFileSystem.CheckSumFSOutputSummer提供了构造FSOutputSummer所需要的参数。
校验和采用PureJavaCrc32，校验和长度4字节，缓冲大小为512字节(默认)。

    public ChecksumFSOutputSummer(ChecksumFileSystem fs,   Path file, boolean overwrite, int bufferSize, 
                                  short replication, long blockSize, Progressable progress) throws IOException {
      super(new PureJavaCrc32(), fs.getBytesPerSum(), 4);
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, replication, blockSize, progress);
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, sumBufferSize, replication, blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }

构造ChecksumFSOutputSummer时，就往校验和文件流中写入魔数CHECKSUM_VERSION("crc\0")和校验块长度。
FSOutputSummer抽象了大部分和数据分块、计算校验和的相关功能，ChecksumFSOutputSummer在此基础上提供了具体的文件流输出。
