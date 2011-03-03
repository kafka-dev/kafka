using System;
using System.Linq;
using System.Text;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Tests
{
    /// <summary>
    /// Tests for the <see cref="ConsumerRequest"/> class.
    /// </summary>
    [TestFixture]
    public class ConsumerRequestTests
    {
        /// <summary>
        /// Tests to ensure that the request follows the expected structure.
        /// </summary>
        [Test]
        public void GetBytesValidStructure()
        {
            string topicName = "topic";
            ConsumerRequest request = new ConsumerRequest(topicName, 1, 10L, 100);

            // REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
            int requestSize = 2 + 2 + topicName.Length + 4 + 8 + 4;

            byte[] bytes = request.GetBytes();
            Assert.IsNotNull(bytes);

            // add 4 bytes for the length of the message at the beginning
            Assert.AreEqual(requestSize + 4, bytes.Length);

            // first 4 bytes = the message length
            Assert.AreEqual(25, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Take(4).ToArray<byte>()), 0));

            // next 2 bytes = the request type
            Assert.AreEqual((short)RequestType.Fetch, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(4).Take(2).ToArray<byte>()), 0));

            // next 2 bytes = the topic length
            Assert.AreEqual((short)topicName.Length, BitConverter.ToInt16(BitWorks.ReverseBytes(bytes.Skip(6).Take(2).ToArray<byte>()), 0));

            // next few bytes = the topic
            Assert.AreEqual(topicName, Encoding.ASCII.GetString(bytes.Skip(8).Take(topicName.Length).ToArray<byte>()));

            // next 4 bytes = the partition
            Assert.AreEqual(1, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(8 + topicName.Length).Take(4).ToArray<byte>()), 0));

            // next 8 bytes = the offset
            Assert.AreEqual(10, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(12 + topicName.Length).Take(8).ToArray<byte>()), 0));

            // last 4 bytes = the max size
            Assert.AreEqual(100, BitConverter.ToInt32(BitWorks.ReverseBytes(bytes.Skip(20 + +topicName.Length).Take(4).ToArray<byte>()), 0));
        }
    }
}
