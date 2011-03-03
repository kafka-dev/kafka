using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Constructs a request to send to Kafka.
    /// </summary>
    public class ConsumerRequest
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int DefaultMaxSize = 1048576;

        /// <summary>
        /// Initializes a new instance of the ConsumerRequest class.
        /// </summary>
        public ConsumerRequest()
        {
        }

        /// <summary>
        /// Initializes a new instance of the ConsumerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        public ConsumerRequest(string topic, int partition, long offset)
            : this(topic, partition, offset, DefaultMaxSize)
        {
        }

        /// <summary>
        /// Initializes a new instance of the ConsumerRequest class.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="offset">The offset in the topic/partition to retrieve from.</param>
        /// <param name="maxSize">The maximum size.</param>
        public ConsumerRequest(string topic, int partition, long offset, int maxSize)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            MaxSize = maxSize;
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
        /// Gets or sets the offset to request.
        /// </summary>
        public long Offset { get; set; }

        /// <summary>
        /// Gets or sets the maximum size to pass in the request.
        /// </summary>
        public int MaxSize { get; set; }

        /// <summary>
        /// Determines if the request has valid settings.
        /// </summary>
        /// <returns>True if valid and false otherwise.</returns>
        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Topic);
        }

        /// <summary>
        /// Gets the bytes matching the expected Kafka structure. 
        /// </summary>
        /// <returns>The byte array of the request.</returns>
        public byte[] GetBytes()
        {
            // REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
            int requestSize = 2 + 2 + Topic.Length + 4 + 8 + 4;

            List<byte> request = new List<byte>();
            request.AddRange(BitWorks.GetBytesReversed(requestSize));
            request.AddRange(BitWorks.GetBytesReversed((short)RequestType.Fetch));
            request.AddRange(BitWorks.GetBytesReversed((short)Topic.Length));
            request.AddRange(Encoding.ASCII.GetBytes(Topic));
            request.AddRange(BitWorks.GetBytesReversed(Partition));
            request.AddRange(BitWorks.GetBytesReversed(Offset));
            request.AddRange(BitWorks.GetBytesReversed(MaxSize));

            return request.ToArray<byte>();
        }
    }
}
