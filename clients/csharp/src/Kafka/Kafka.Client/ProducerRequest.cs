using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ProducerRequest
    {
        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        public ProducerRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the ProducerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public ProducerRequest(string topic, int partition, IList<Message> messages)
        {
            Topic = topic;
            Partition = partition;
            Messages = messages;
        }

        /// <summary>
        /// Gets or sets the topic to publish to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Gets or sets the partition to publish to.
        /// </summary>
        public int Partition { get; set; }

        /// <summary>
        /// Gets or sets the messages to publish.
        /// </summary>
        public IList<Message> Messages { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic) && Messages != null && Messages.Count > 0;
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public byte[] GetBytes()
        {
            List<byte> messagePack = new List<byte>();
            foreach (Message message in Messages)
            {
                byte[] messageBytes = message.GetBytes();
                messagePack.AddRange(BitWorks.GetBytesReversed(messageBytes.Length));
                messagePack.AddRange(messageBytes);
            }

            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Produce));
            byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(Topic.Length));
            byte[] topicBytes = Encoding.UTF8.GetBytes(Topic);
            byte[] partitionBytes = BitWorks.GetBytesReversed(Partition);
            byte[] messagePackLengthBytes = BitWorks.GetBytesReversed(messagePack.Count);
            byte[] messagePackBytes = messagePack.ToArray();

            List<byte> encodedMessageSet = new List<byte>();
            encodedMessageSet.AddRange(requestBytes);
            encodedMessageSet.AddRange(topicLengthBytes);
            encodedMessageSet.AddRange(topicBytes);
            encodedMessageSet.AddRange(partitionBytes);
            encodedMessageSet.AddRange(messagePackLengthBytes);
            encodedMessageSet.AddRange(messagePackBytes);
            encodedMessageSet.InsertRange(0, BitWorks.GetBytesReversed(encodedMessageSet.Count));

            return encodedMessageSet.ToArray();
        }
    }
}
