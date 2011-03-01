using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Client.Util
{
    /// <summary>
    /// Utilty class for managing bits and bytes.
    /// </summary>
    public class BitWorks
    {
        /// <summary>
        /// Reverse the position of an array of bytes.
        /// </summary>
        /// <param name="inArray">
        /// The array to reverse.  If null or zero-length then the returned array will be null.
        /// </param>
        /// <returns>The reversed array.</returns>
        public static byte[] ReverseBytes(byte[] inArray)
        {
            if (inArray != null && inArray.Length > 0)
            {
                int highCtr = inArray.Length - 1;
                byte temp;

                for (int ctr = 0; ctr < inArray.Length / 2; ctr++)
                {
                    temp = inArray[ctr];
                    inArray[ctr] = inArray[highCtr];
                    inArray[highCtr] = temp;
                    highCtr -= 1;
                }
            }

            return inArray;
        }
    }
}
