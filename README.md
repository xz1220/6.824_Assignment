# 6.824 分布式系统

相关地址：[官方课程地址](https://pdos.csail.mit.edu/6.824/index.html)  [B站搬运课](https://www.bilibili.com/video/BV1qk4y197bB)

## MapReduce & lab1
### 基本理论与摘要

lab1需要用go 协程去实现一个分布式的mapreduce程序，已经给出了串行的情况。需要自己实现一下coordinator、worker、rpc三个组件，在src/mr下。

#### 串行MapReduce示例

示例程序在src/main/mrsequence.go 下，主要流程如下：

- 读取文件内容并调用map函数生成k-v形式的中间结果，k为单词字符串，v为1
- 按照K进行排序，相同的k会在排序中被划分到一起
- 找到相同的k的开始下标和结束下标，传递这个切片到reduce函数中
- reduce切片长度并返回，切片长度即单词出现的次数

上述流程对于单纯的统计字符串来说，有些冗杂。但是，其代表了一个通用的计算模型。

#### Lab1

在着手实现之前，需要通读一下论文部分。主要着重点在于第三章实现和第四章优化部分。由于需要进行复现，所以阅读的时候需要注意一下几点。

- Coordinator的职责是什么，保存怎样的数据结构，如何划分和监控任务的执行
- worker的职责是什么，保存怎样的数据接口
- worker和Coordinator之间如何进行RPC调用

> [MapReduce 论文地址](http://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf)

![MapReduce- execution overview](./doc/img/MapReduce- execution overview.png)

<details>
<summary><strong> 集群环境 </strong></summary>

</details>



<!-- template -->
<!-- <details>
<summary><strong>  </strong></summary>
</details> -->

