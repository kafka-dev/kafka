<?php

/**
 * Description of FetchRequest
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_FetchRequest extends Kafka_Request
{
	private $topic;
	private $partition;
	private $offset;
	private $maxSize;
	
	/**
	 * @param string  $topic
	 * @param integer $partition
	 * @param integer $offset
	 * @param integer $maxSize 
	 */
	public function __construct($topic, $partition = 0, $offset = 0, $maxSize = 1000000) {
		$this->id        = Kafka_RequestKeys::FETCH;
		$this->topic     = $topic;
		$this->partition = $partition;
		$this->offset    = $offset;
		$this->maxSize   = $maxSize;
	}
	
	/**
	 * @param resource $stream 
	 */
	public function writeTo($stream) {
		//echo "\nWriting request to stream: " . (string)$this;
		// <topic size: short> <topic: bytes>
		fwrite($stream, pack('n', strlen($this->topic)) . $this->topic);
		// <partition: int> <offset: Long> <maxSize: int>
		fwrite($stream, pack('N', $this->partition));
		
//TODO: need to store a 64bit integer (bigendian), but PHP only supports 32bit integers: setting first 32 bits to 0
		fwrite($stream, pack('N2', 0, $this->offset));
		fwrite($stream, pack('N', $this->maxSize));
		//echo "\nWritten request to stream: " .(string)$this;
	}
	
	/**
	 * @return integer
	 */
	public function sizeInBytes() {
		return 2 + strlen($this->topic) + 4 + 8 + 4;
	}
	
	/**
	 * @return integer
	 */
	public function getOffset() {
		return $this->offset;
	}
	
	/**
	 * @return string
	 */
	public function getTopic() {
		return $this->topic;
	}
	
	/**
	 * @return integer
	 */
	public function getPartition() {
		return $this->partition;
	}
	
	/**
	 * @return string
	 */
	public function __toString()
	{
		return 'topic:' . $this->topic . ', part:' . $this->partition . ' offset:' . $this->offset . ' maxSize:' . $this->maxSize;
	}
}

