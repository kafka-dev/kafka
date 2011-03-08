# Kafka is a distributed publish/subscribe messaging system #

It is designed to support the following

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware Kafka can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over Kafka servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Support for parallel data load into Hadoop.

Kafka is aimed at providing a publish-subscribe solution that can handle all activity stream data and processing on a consumer-scale web site. This kind of activity (page views, searches, and other user actions) are a key ingredient in many of the social feature on the modern web. This data is typically handled by "logging" and ad hoc log aggregation solutions due to the throughput requirements. This kind of ad hoc solution is a viable solution to providing logging data to an offline analysis system like Hadoop, but is very limiting for building real-time processing. Kafka aims to unify offline and online processing by providing a mechanism for parallel load into Hadoop as well as the ability to partition real-time consumption over a cluster of machines.

See our [web site](http://sna-projects.com/kafka) for more details on the project.

## Contribution ##

Kafka is a new project, and we are interested in building the community; we would welcome any thoughts or patches. You can reach us [here](http://groups.google.com/group/kafka-dev). 

To get kafka code:
  git clone git@github.com:kafka-dev/kafka.git kafka

To build: 

1. for the first time: ./sbt update compile
2. else: ./sbt compile

To run unit tests:
  ./sbt test


