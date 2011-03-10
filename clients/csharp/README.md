# .NET Kafka Client

This is a .NET implementation of a client for Kafka using C#.  It provides for a basic implementation that covers most basic functionalities to include a simple Producer and Consumer.

Exceptions are not trapped within the library and basically bubble up directly from the TcpClient and it's underlying Socket connection.  Clients using this library should look to do their own exception handling.

## Producer

The Producer can send one or more messages to Kafka in both a synchronous and asynchronous fashion.

### Producer Usage

    string payload1 = "kafka 1.";
    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
    Message msg1 = new Message(payloadData1);

    string payload2 = "kafka 2.";
    byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
    Message msg2 = new Message(payloadData2);

    Producer producer = new Producer("localhost", 9092);
    producer.Send("test", 0, new List<Message> { msg1, msg2 });

### Asynchronous Producer Usage

    List<Message> messages = GetBunchOfMessages();

    Producer producer = new Producer("localhost", 9092);
    producer.SendAsync("test", 0, messages, (requestContext) => { // doing work });

## Consumer

Currently, the consumer is not terribly advanced.  It has two functions of interest: `GetOffsetsBefore` and `Consume`.  `GetOffsetsBefore` will retrieve a list of offsets before a given time and `Consume` will attempt to get a list of messages from Kafka given a topic, partition and offset.

### Consumer Usage

    Consumer consumer = new Consumer("localhost", 9092);
    int max = 10;
    long[] offsets = consumer.GetOffsetsBefore("test", 0, OffsetRequest.LatestTime, max);
    List<Message> messages = consumer.Consume("test", 0, offsets[0]);