Apache Kafka
============

Apache Kafka is - a publish-subscribe-based messaging system that is exchanging data between processes,
applications, and servers. In this presentation, I will give you a brief understanding of messaging and distributed logs,
and important concepts will be defined.

Publish-subscribe messaging system
----------------------------------

Apache Kafka is a publish-subscribe messaging system. By saying that, we need to describe a messaging system.
A messaging system lets you send messages between processes, applications, and servers.

![](https://raw.githubusercontent.com/ksekou/kafka-demo/master/images/kafka-message-queue.png)


Kafka Broker
------------

A Kafka cluster consists of one or more servers (Kafka brokers), which are running Kafka. Producers are processes that publish data (push messages) into Kafka topics within the broker. A consumer of topics pulls messages off a Kafka topic.


Solarized dark             |  Solarized Ocean
:-------------------------:|:-------------------------:
![](https://github.com/ksekou/kafka-demo/blob/master/images/apache-kafka-getting-started.png?raw=true)  |  ![](https://github.com/ksekou/kafka-demo/blob/master/images/kafka-producer-consumer-broker.png?raw=true)


Kafka Topic
-----------

A Topic is a category/feed name to which messages are stored and published. Messages are byte arrays that can store any object in any format. As said before, all Kafka messages are organized into topics. If you wish to send a message you send it to a specific topic and if you wish to read a message you read it from a specific topic. Producer applications write data to topics and consumer applications read from topics. Messages published to the cluster will stay in the cluster until a configurable retention period has passed by. Kafka retains all messages for a set amount of time, and therefore, consumers are responsible to track their location.

![](https://github.com/ksekou/kafka-demo/blob/master/images/apache-kafka-topic.png?raw=true)

Kafka topic partition
---------------------

Kafka topics are divided into a number of partitions, which contains messages in an unchangeable sequence. Each message in a partition is assigned and identified by its unique offset. A topic can also have multiple partition logs like the click-topic has in the image to the right. This allows for multiple consumers to read from a topic in parallel.

In Kafka, replication is implemented at the partition level. The redundant unit of a topic partition is called a replica. Each partition usually has one or more replicas meaning that partitions contain messages that are replicated over a few Kafka brokers in the cluster. As we can see in the pictures - the click-topic is replicated to Kafka node 2 and Kafka node 3.

It's possible for the producer to attach a key to the messages and tell which partition the message should go to. All messages with the same key will arrive at the same partition.

Partitions allow you to parallelize a topic by splitting the data in a particular topic across multiple brokers.

Every partition (replica) has one server acting as a leader and the rest of them as followers. The leader replica handles all read-write requests for the specific partition and the followers replicate the leader. If the leader server fails, one of the follower servers become the leader by default. When a producer publishes a message to a partition in a topic, it is forwarded to its leader. The leader appends the message to its commit log and increments its message offset. Kafka only exposes a message to a consumer after it has been committed and each piece of data that comes in will be stacked on the cluster.

![](https://github.com/ksekou/kafka-demo/blob/master/images/kafka-partition-in-topic.png?raw=true)


Consumers and consumer groups
-----------------------------

Consumers can read messages starting from a specific offset and are allowed to read from any offset point they choose. This allows consumers to join the cluster at any point in time.

![](https://github.com/ksekou/kafka-demo/blob/master/images/kafka-consumer.png?raw=true)


Apache Kafka and server concepts
--------------------------------

Here are important concepts that you need to remember before we dig deeper into Apache Kafka - explained in one line.


- Producer: Application that sends the messages.
- Consumer: Application that receives the messages.
- Message: Information that is sent from the producer to a consumer through Apache Kafka.
- Connection: A connection is a TCP connection between your application and the Kafka broker.
- Topic: A Topic is a category/feed name to which messages are stored and published.
- Topic partition: Kafka topics are divided into a number of partitions, which allows you to split data across multiple brokers.
Replicas A replica of a partition is a "backup" of a partition. Replicas never read or write data. They are used to prevent data loss.
- Consumer Group: A consumer group includes the set of consumer processes that are subscribing to a specific topic.
- Offset: The offset is a unique identifier of a record within a partition. It denotes the position of the consumer in the partition.
- Node: A node is a single computer in the Apache Kafka cluster.
- Cluster: A cluster is a group of nodes i.e., a group of computers.
