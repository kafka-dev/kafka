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
        /// <returns>The data read from the server as a byte array.</returns>
        public byte[] Read()
        {
            // Get the stream used to read the message sent by the server.
            NetworkStream networkStream = _client.GetStream();

            // Set a 10 millisecond timeout for reading.
            networkStream.ReadTimeout = 10;
            
            // Read the server message into a byte buffer.
            byte[] bytes = new byte[1024];
            networkStream.Read(bytes, 0, 1024);

            networkStream.Close();

            return bytes;
        }

        /// <summary>
        /// Writes data to the server.
        /// </summary>
        /// <param name="data">The data to write to the server.</param>
        public void Write(byte[] data)
        {
            using (NetworkStream stream = _client.GetStream())
            {
                // Send the message to the connected TcpServer. 
                stream.Write(data, 0, data.Length);
            }
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
