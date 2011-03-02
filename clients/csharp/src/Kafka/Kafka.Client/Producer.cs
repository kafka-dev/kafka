using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Util;

namespace Kafka.Client
{
    /// <summary>
    /// Sends message to Kafka.
    /// </summary>
    public class Producer
    {
        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public Producer(string server, int port)
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
        /// Sends a message to Kafka.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="msg">The message to send.</param>
        public void Send(string topic, int partition, Message msg)
        {
            Send(topic, partition, new List<Message> { msg });
        }

        /// <summary>
        /// Sends a list of messages to Kafka.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="partition">The partition to publish to.</param>
        /// <param name="messages">The list of messages to send.</param>
        public void Send(string topic, int partition, IList<Message> messages)
        {
            using (KafkaConnection connection = new KafkaConnection(Server, Port))
            {
                byte[] request = EncodeMessages(topic, partition, messages);
                connection.Write(request);
            }
        }

        /// <summary>
        /// Encodes messages to be understood by the Kafka server.
        /// </summary>
        /// <param name="topic">The topic to encode.</param>
        /// <param name="partition">The partition to encode.</param>
        /// <param name="messages">The messages to encode.</param>
        /// <returns>Bytes representing the request to send to the server.</returns>
        private byte[] EncodeMessages(string topic, int partition, IList<Message> messages)
        {
            List<byte> messagePack = new List<byte>();
            foreach (Message message in messages)
            {
                byte[] messageBytes = message.GetBytes();
                messagePack.AddRange(BitWorks.GetBytesReversed(messageBytes.Length));
                messagePack.AddRange(messageBytes);
            }

            byte[] requestBytes = BitWorks.GetBytesReversed(Convert.ToInt16((int)RequestType.Produce));
            byte[] topicLengthBytes = BitWorks.GetBytesReversed(Convert.ToInt16(topic.Length));
            byte[] topicBytes = Encoding.UTF8.GetBytes(topic);
            byte[] partitionBytes = BitWorks.GetBytesReversed(partition);
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
