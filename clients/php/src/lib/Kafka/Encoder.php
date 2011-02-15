<?php

/**
 * Description of Encoder
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_Encoder
{
	/**
	 * 1 byte "magic" identifier to allow format changes
	 * 
	 * @var integer
	 */
	const CURRENT_MAGIC_VALUE = 0;
	
	/**
	 * Encode a message. The format of an N byte message is the following:
     *  - 1 byte: "magic" identifier to allow format changes
     *  - 4 bytes: CRC32 of the payload
     *  - (N - 5) bytes: payload
	 * 
	 * 
	 * @param string $msg
	 *
	 * @return string
	 */
	static public function encode_message($msg) {
		// <MAGIC_BYTE: 1 byte> <CRC32: 4 bytes bigendian> <PAYLOAD: N bytes>
		return pack('CN', self::CURRENT_MAGIC_VALUE, crc32($msg)) 
			 . $msg;
	}

	/**
	 * @param string  $topic
	 * @param integer $partition
	 * @param array   $messages
	 *
	 * @return string
	 */
	static public function encode_produce_request($topic, $partition, array $messages) {
		// encode messages as <LEN: int><MESSAGE_BYTES>
		$message_set = '';
		foreach ($messages as $message) {
			$encoded = self::encode_message($message);
			$message_set .= pack('N', strlen($encoded)) . $encoded;
		}
		// create the request as <REQUEST_SIZE: int> <REQUEST_ID: short> <TOPIC: bytes> <PARTITION: int> <BUFFER_SIZE: int> <BUFFER: bytes>
		$data = pack('n', PRODUCE_REQUEST_ID) .
			pack('n', strlen($topic)) . $topic .
			pack('N', $partition) .
			pack('N', strlen($message_set)) . $message_set;
		return pack('N', strlen($data)) . $data;
	}
}
