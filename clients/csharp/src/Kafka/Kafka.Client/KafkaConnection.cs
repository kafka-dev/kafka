using System;
using System.Net.Sockets;

namespace Kafka.Client
{
    /// <summary>
    /// Manages connections to the Kafka.
    /// </summary>
    public class KafkaConnection : IDisposable
    {
        /// <summary>
        /// TCP client that connects to the server.
        /// </summary>
        private TcpClient _client;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="server">The server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        public KafkaConnection(string server, int port)
        {
            Server = server;
            Port = port;

            // connection opened
            _client = new TcpClient(server, port);
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
        /// Readds data from the server.
        /// </summary>
        /// <param name="size">The number of bytes to read from the server.</param>
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read(int size)
        {
            NetworkStream stream = _client.GetStream();

            // Set a 10 millisecond timeout for reading.
            stream.ReadTimeout = 10;

            // Read the server message into a byte buffer.
            byte[] bytes = new byte[size];
            stream.Read(bytes, 0, size);

            return bytes;
        }

        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <param name="data">The data to write to the server.</param>
        public void Write(byte[] data)
        {
            NetworkStream stream = _client.GetStream();

            // Send the message to the connected TcpServer. 
            stream.Write(data, 0, data.Length);
        }

        /// <summary>
        /// Writes a producer request to the server.
        /// </summary>
        /// <param name="request">The <see cref="ProducerRequest"/> to send to the server.</param>
        public void Write(ProducerRequest request)
        {
            Write(request.GetBytes());
        }

        /// <summary>
        /// Close the connection to the server.
        /// </summary>
        public void Dispose()
        {
            if (_client != null)
            {
                _client.Close();
            }
        }
    }
}
