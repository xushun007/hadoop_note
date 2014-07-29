# ByteBuffer

徐顺 2014-07-15

## ByteBuffer 概览
ByteBuffer抽象类是Java NIO里用得最多的Buffer，此类定义了除 boolean 之外，读写所有其他基本类型值的方法。它包含两个实现方式：
HeapByteBuffer是基于Java堆的实现，ByteBuffer.allocate(int)返回的是HeapByteBuffer.
DirectByteBuffer则使用了unsafe的API进行了堆外的实现，ByteBuffer.allocateDirect(int)返回的是DirectByteBuffer。 

HeapByteBuffer分配在jvm的堆（如新生代）上，和其它对象一样，由gc来扫描、回收。
DirectByteBuffer的内容可以驻留在常规的垃圾回收堆之外，因此，它们对应用程序的内存需求量造成的影响可能并不明显。所以，建议将直接缓冲区主要分配给那些易受基础系统的本机 I/O 操作影响的大型、持久的缓冲区。

非线程安全

## ByteBuffer 继承结构
	public abstract class ByteBuffer
	extends Buffer
	implements Comparable<ByteBuffer>

## ByteBuffer 属性

* buff
  buff即内部用于缓存的数组。
* capacity
  ByteBuffer的存储容量。
* limit
  在Buffer上进行的读写操作都不能越过这个下标。
  写数据到buffer中时，limit一般和capacity相等；当读数据时，limit代表buffer中有效数据的长度。
* position
  读/写操作的当前下标。
  当使用buffer的相对位置进行读/写操作时，读/写会从这个下标进行，并在操作完成后，buffer会更新下标的值。
* mark
  一个临时存放的位置下标，便于某些时候回到该位置。
  调用mark()会将mark设为当前的position的值，以后调用reset()会将position属性设置为mark的值。

不变式：0 <= mark <= position <= limit <= capacity

### put()
将给定的字节写入此缓冲区的当前位置，然后该位置递增。

### flip()
反转此缓冲区。首先将限制设置为当前位置，然后将位置设置为 0。如果已定义了标记，则丢弃该标记。

    public final Buffer flip() {
		limit = position;
		position = 0;
		mark = -1;
		return this;
    }

在一系列通道读取或放置 操作之后，调用此方法为一系列通道写入或相对获取 操作做好准备。例如：

	buf.put(magic);    // Prepend header
	in.read(buf);      // Read data into rest of buffer
	buf.flip();        // Flip buffer
	out.write(buf);    // Write header + data to channel

当将数据从一个地方传输到另一个地方时，经常将此方法与 compact()方法一起使用。


### get()
从buffer里读一个字节，并把postion移动一位。上限是limit，即写入数据的最后位置。 

### clear()
清除此缓冲区。将位置设置为 0，将限制设置为容量，并丢弃标记。

    public final Buffer clear() {
		position = 0;
		limit = capacity;
		mark = -1;
		return this;
    } 

### rewind()
重绕此缓冲区。将位置设置为 0 并丢弃标记。

    public final Buffer rewind() {
	position = 0;
	mark = -1;
	return this;
    }

在一系列通道写入或获取 操作之前调用此方法（假定已经适当设置了限制）。例如：

	out.write(buf);    // Write remaining data
	buf.rewind();      // Rewind buffer
	buf.get(array);    // Copy data into array    

### compact()
将position到limit的数据复制到缓冲区的开始位置，为后续的put()/read()调用让出空间。
    
    public ByteBuffer compact() {
		System.arraycopy(hb, ix(position()), hb, ix(0), remaining());
		position(remaining());
		limit(capacity());
		return this;
    }

    public final int remaining() {
		return limit - position;
    }

### mark() 
在此缓冲区的位置设置标记(mark=position)。

### reset()
将position设置为mark的值(如果mark>=0)。



ByteBuffer的底层结构清晰，不复杂，源码仍是弄清原理的最佳文档。