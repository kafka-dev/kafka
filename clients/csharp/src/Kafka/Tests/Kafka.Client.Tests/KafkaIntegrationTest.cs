using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
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
        /// Asynchronously sends a pair of message to Kafka.
        /// </summary>
        [Test]
        public void ProducerSendsMessageAsynchronously()
        {
            bool waiting = true;

            List<Message> messages = GenerateRandomMessages(50);

            Producer producer = new Producer("192.168.50.202", 9092);
            producer.SendAsync(
                "test",
                0,
                messages,
                (requestContext) => { waiting = false; });

            while (waiting)
            {
                Console.WriteLine("Keep going...");
                Thread.Sleep(10);
            }
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

        /// <summary>
        /// Gets offsets from Kafka.
        /// </summary>
        [Test]
        public void ConsumerGetsOffsets()
        {
            OffsetRequest request = new OffsetRequest("test", 0, DateTime.Now.AddHours(-6).Ticks, 10);

            Consumer consumer = new Consumer("192.168.50.202", 9092);
            IList<long> list = consumer.GetOffsetsBefore(request);

            foreach (long l in list)
            {
                Console.Out.WriteLine(l);
            }
        }

        /// <summary>
        /// Gererates a randome list of messages.
        /// </summary>
        /// <param name="numberOfMessages">The number of messages to generate.</param>
        /// <returns>A list of random messages.</returns>
        private static List<Message> GenerateRandomMessages(int numberOfMessages)
        {
            List<Message> messages = new List<Message>();
            for (int ix = 0; ix < numberOfMessages; ix++)
            {
                messages.Add(new Message(GenerateRandomBytes(10000)));
            }

            return messages;
        }

        /// <summary>
        /// Generate a random set of bytes.
        /// </summary>
        /// <param name="length">Length of the byte array.</param>
        /// <returns>Random byte array.</returns>
        private static byte[] GenerateRandomBytes(int length)
        {
            byte[] randBytes = new byte[length];
            Random randNum = new Random();
            randNum.NextBytes(randBytes);

            return randBytes;
        }
    }
}
