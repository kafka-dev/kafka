using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Kafka.Client.Tests
{
    /// <summary>
    /// Contains tests that go all the way to Kafka and back.
    /// </summary>
    [TestFixture]
    public class KafkaIntegrationTest
    {
        /// <summary>
        /// Sends a pair of message to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessage()
        {
            string payload1 = "kafka 1.";
            byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
            Message msg1 = new Message(payloadData1);

            string payload2 = "kafka 2.";
            byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
            Message msg2 = new Message(payloadData2);

            Producer producer = new Producer("192.168.50.202", 9092);
            producer.Send("test", 0, new List<Message> { msg1, msg2 });
        }

        /// <summary>
        /// Generates messages for Kafka then gets them back.
        /// </summary>
        [Test]
        public void ConsumerGetsMessage()
        {
            ProducerSendsMessage();

            Consumer consumer = new Consumer("192.168.50.202", 9092);
            consumer.Consume("test", 0, 0);
        }
    }
}
