<?php

/**
 * A message. The format of an N byte message is the following:
 * 1 byte "magic" identifier to allow format changes
 * 4 byte CRC32 of the payload
 * N - 5 byte payload
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_Message
{
	/*
	private $currentMagicValue = Kafka_Encoder::CURRENT_MAGIC_VALUE;
	private $magicOffset   = 0;
	private $magicLength   = 1;
	private $crcOffset     = 1; // MagicOffset + MagicLength
	private $crcLength     = 4;
	private $payloadOffset = 5; // CrcOffset + CrcLength
	private $headerSize    = 5; // PayloadOffset
	*/
	
	private $payload = null;
	private $size    = 0;
	private $crc     = false;
	
	/**
	 * @param string $data
	 */
	public function __construct($data) {
		$this->payload = substr($data, 5);
		$this->crc     = crc32($this->payload);
		$this->size    = strlen($this->payload);
	}
	
	/**
	 * @return string
	 */
	public function encode() {
		return Kafka_Encoder::encode_message($this->payload);
	}
	
	/**
	 * @return integer
	 */
	public function size() {
		return $this->size;
	}
  
	/**
	 * @return integer
	 */
	public function magic() {
		return Kafka_Encoder::CURRENT_MAGIC_VALUE;
	}
	
	/**
	 * @return integer
	 */
	public function checksum() {
		return $this->crc;
	}
	
	/**
	 * @return string
	 */
	public function payload() {
		return $this->payload;
	}
	
	/**
	 * @return boolean
	 */
	public function isValid() {
		return ($this->crc === crc32($this->payload));
	}
  
	/**
	 * @return string
	 */
	public function __toString() {
		return 'message(magic = ' . Kafka_Encoder::CURRENT_MAGIC_VALUE . ', crc = ' . $this->crc .
			', payload = ' . $this->payload . ')';
	}
}
