# .NET Kafka Client

## Producer Usage

    string payload1 = "kafka 1.";
    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
    Message msg1 = new Message(payloadData1);

    string payload2 = "kafka 2.";
    byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
    Message msg2 = new Message(payloadData2);

    Producer producer = new Producer("192.168.50.201", 9092);
    producer.Send("test", 0, new List<Message> { msg1, msg2 });

## Consumer Usage

    Consumer consumer = new Consumer("192.168.50.202", 9092);
    List<Message> messages = consumer.Consume("test", 0, 0);