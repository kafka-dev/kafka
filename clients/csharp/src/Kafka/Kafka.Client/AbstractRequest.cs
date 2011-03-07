using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client
{
    /// <summary>
    /// Base request to make to Kafka.
    /// </summary>
    public abstract class AbstractRequest
    {
        /// <summary>
        /// Gets or sets the topic to publish to.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// Gets or sets the partition to publish to.
        /// </summary>
        public int Partition { get; set; }
    }
}
