# Hadoop笔记大纲

徐顺  2014-06

## 简介

自己对NoSQL、大数据存储与处理比较感兴趣，先后阅读了Memcached和Redis核心源码，对Hadoop很早比较感兴趣，2011年研一下学期实验室一位博士师兄曾对我说，你想找份好工作，看完Hadoop源码应该差不多了，那时翻了翻《Hadoop权威指南》，对Hadoop的了解也没继续读下去了。

由于现在在公司基本不需要加班，自己可支配的时间较多，对感兴趣的技术比较喜欢了解底层到底是怎么实现的，海量数据处理也是未来的重要方向之一。

Hadoop基于Java编写，Java是我最熟练的语言，所以阅读源码比以前看Redis要轻松一些。当初学习Java(2006年)的初衷是几行Java代码就可以写一个UI，觉得很牛的样子，然而Swing几乎没人用了。

在此记下看Hadoop源码和书籍的一些笔记。

## 大纲

* [Hadoop 安装](Hadoop_Install.md)
* [Hadoop IPC原理](Hadoop_IPC.md)
* [Hadoop 文件系统设计](Hadoop_FileSystem_Design.md)
* [Hadoop FileSystem & ChecksumFileSystem](Hadoop_FileSystem_Impl.md)
* [Hadoop 远程过程调用接口 - 服务器间接口](Hadoop_HDFS_Invoke_Interface_Server.md)
* [Hadoop 远程过程调用接口 - 客户端接口](Hadoop_HDFS_NonIPC_Interface_Client.md)
* [Hadoop 流式接口](Hadoop_HDFS_NonIPC_Interface_Client.md)
* [Hadoop 数据节点之读数据](Hadoop_DataNode_ReadData.md)
* [Hadoop 数据节点之写数据](Hadoop_DataNode_WriteData.md)
* [Hadoop 名字节点 - 数据结构](Hadoop_NameNode_DataStruture.md)
* [Hadoop 名字节点 - 读写数据](Hadoop_NameNode_DR.md)
* [Hadoop HDFS漫画](Hadoop_HDFS_Comics.md)
* ...
* [ByteBuffer简介](ByteBuffer.md)
* 未完，待续...

主要参考资料

> http://hadoop.apache.org/

> Hadoop技术内幕-深入解析Hadoop Common和HDFS架构设计与实现原理

> Hadoop技术内幕-深入解析MapReduce架构设计与实现原理