using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Consumes messages from Kafka.
    /// </summary>
    public class Consumer
    {
        /// <summary>
        /// Maximum size.
        /// </summary>
        private static readonly int MaxSize = 1048576;

        /// <summary>
        /// Initializes a new instance of the Consumer class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public Consumer(string server, int port)
        {
            Server = server;
            Port = port;
        }

        /// <summary>
        /// Gets the server to which the connection is to be established.
        /// </summary>
        public string Server { get; private set; }

        /// <summary>
        /// Gets the port to which the connection is to be established.
        /// </summary>
        public int Port { get; private set; }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="topic">The topic to consume from.</param>
        /// <param name="partition">The partition to consume from.</param>
        /// <param name="offset">The offset to start at.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(string topic, int partition, long offset)
        {
            return Consume(topic, partition, offset, MaxSize);
        }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="topic">The topic to consume from.</param>
        /// <param name="partition">The partition to consume from.</param>
        /// <param name="offset">The offset to start at.</param>
        /// <param name="maxSize">The maximum size.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(string topic, int partition, long offset, int maxSize)
        {
            return Consume(new ConsumerRequest(topic, partition, offset, maxSize));
        }

        /// <summary>
        /// Consumes messages from Kafka.
        /// </summary>
        /// <param name="request">The request to send to Kafka.</param>
        /// <returns>A list of messages from Kafka.</returns>
        public List<Message> Consume(ConsumerRequest request)
        {
            List<Message> messages = new List<Message>();
            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                connection.Write(request.GetBytes());
                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);

                if (dataLength > 0) 
                {
                    byte[] data = connection.Read(dataLength);

                    // TODO: need to check in on kafka error codes...assume all's good for now
                    byte[] unbufferedData = data.Skip(2).ToArray();

                    int processed = 0;
                    int length = unbufferedData.Length - 4;
                    int messageSize = 0;
                    while (processed <= length) 
                    {
                        messageSize = BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Skip(processed).Take(4).ToArray<byte>()), 0);
                        messages.Add(Message.ParseFrom(unbufferedData.Skip(processed).Take(messageSize + 4).ToArray<byte>()));
                        processed += 4 + messageSize;
                    }
                }
            }

            return messages;
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="topic">The topic to check.</param>
        /// <param name="partition">The partition on the topic.</param>
        /// <param name="time">time in millisecs (if -1, just get from the latest available)</param>
        /// <param name="maxNumOffsets">That maximum number of offsets to return.</param>
        /// <returns>List of offsets, in descending order.</returns>
        public IList<long> GetOffsetsBefore(string topic, int partition, long time, int maxNumOffsets)
        {
            return GetOffsetsBefore(new OffsetRequest(topic, partition, time, maxNumOffsets));
        }

        /// <summary>
        /// Get a list of valid offsets (up to maxSize) before the given time.
        /// </summary>
        /// <param name="request">The offset request.</param>
        /// <returns>List of offsets, in descending order.</returns>
        public IList<long> GetOffsetsBefore(OffsetRequest request)
        {
            List<long> offsets = new List<long>();

            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                connection.Write(request.GetBytes());

                int dataLength = BitConverter.ToInt32(BitWorks.ReverseBytes(connection.Read(4)), 0);
                
                if (dataLength > 0)
                {
                    byte[] data = connection.Read(dataLength);

                    // TODO: need to check in on kafka error codes...assume all's good for now
                    byte[] unbufferedData = data.Skip(2).ToArray();

                    // first four bytes are the number of offsets
                    int numOfOffsets = BitConverter.ToInt32(BitWorks.ReverseBytes(unbufferedData.Take(4).ToArray<byte>()), 0);

                    int position = 0;
                    for (int ix = 0; ix < numOfOffsets; ix++)
                    {
                        position = (ix * 8) + 4;
                        offsets.Add(BitConverter.ToInt64(BitWorks.ReverseBytes(unbufferedData.Skip(position).Take(8).ToArray<byte>()), 0));
                    }
                }
            }

            return offsets;
        }
    }
}
