using System.Collections.Generic;
using NUnit.Framework;

namespace Kafka.Client.Tests
{
    /// <summary>
    /// Tests for the <see cref="ProducerRequest"/> class.
    /// </summary>
    [TestFixture]
    public class ProducerRequestTests
    {
        /// <summary>
        /// Tests a valid producer request.
        /// </summary>
        [Test]
        public void IsValidTrue()
        {
            ProducerRequest request = new ProducerRequest(
                "topic", 0, new List<Message> { new Message(new byte[10]) });
            Assert.IsTrue(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid producer request with no topic.
        /// </summary>
        [Test]
        public void IsValidFalseNoTopic()
        {
            ProducerRequest request = new ProducerRequest(null, 0, null);
            Assert.IsFalse(request.IsValid());
        }

        /// <summary>
        /// Tests a invalid producer request with no messages to send.
        /// </summary>
        [Test]
        public void IsValidFalseNoMessages()
        {
            ProducerRequest request = new ProducerRequest("topic", 0, null);
            Assert.IsFalse(request.IsValid());
        }
    }
}
