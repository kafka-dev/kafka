using System;
using Kafka.Client.Util;
using NUnit.Framework;

namespace Kafka.Client.Tests.Util
{
    /// <summary>
    /// Tests for <see cref="BitWorks"/> utility class.
    /// </summary>
    [TestFixture]
    public class BitWorksTests
    {
        /// <summary>
        /// Null array will reverse to a null.
        /// </summary>
        [Test]
        public void ReverseBytesNullArray()
        {
            byte[] arr = null;
            Assert.IsNull(BitWorks.ReverseBytes(arr));
        }

        /// <summary>
        /// Zero length array will reverse to a zero length array.
        /// </summary>
        [Test]
        public void ReverseBytesZeroLengthArray()
        {
            byte[] arr = new byte[0];
            byte[] reversedArr = BitWorks.ReverseBytes(arr);
            Assert.IsNotNull(reversedArr);
            Assert.AreEqual(0, reversedArr.Length);
        }

        /// <summary>
        /// Array is reversed.
        /// </summary>
        [Test]
        public void ReverseBytesValid()
        {
            byte[] arr = BitConverter.GetBytes((short)1);
            byte[] original = new byte[2];
            arr.CopyTo(original, 0);
            byte[] reversedArr = BitWorks.ReverseBytes(arr);

            Assert.IsNotNull(reversedArr);
            Assert.AreEqual(2, reversedArr.Length);
            Assert.AreEqual(original[0], reversedArr[1]);
            Assert.AreEqual(original[1], reversedArr[0]);
        }
    }
}
