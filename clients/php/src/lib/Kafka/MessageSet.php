<?php

/**
 * A sequence of messages stored in a byte buffer
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_MessageSet implements Iterator {
	
	/**
	 * @var integer
	 */
	protected $validByteCount = 0;
	
	/**
	 * @var boolean
	 */
	private $valid = false;
	
	/**
	 * @var array
	 */
	private $array = array();
	
	/**
	 * @param resource $stream
	 * @param integer  $errorCode
	 */
	public function __construct($stream, $errorCode = 0) {
		$data = stream_get_contents($stream);
		$len = strlen($data);
		$ptr = 0;
		while ($ptr <= ($len - 4)) {
			$size = array_shift(unpack('N', substr($data, $ptr, 4)));
			$ptr += 4;
			$this->array[] = new Kafka_Message(substr($data, $ptr, $size));
			$ptr += $size;
			$this->validByteCount += 4 + $size;
		}
		fclose($stream);
	}
	
	/**
	 * @return integer
	 */
	public function validBytes() {
		return $this->validByteCount;
	}
	
	/**
	 * @return integer
	 */
	public function sizeInBytes() {
		return $this->validBytes();
	}
	
	/**
	 * 
	 */
	public function next() {
		$this->valid = (FALSE !== next($this->array)); 
	}	
	
	/**
	 * @return boolean
	 */
	public function valid() {
		return $this->valid;
	}
	
	/**
	 * @return integer
	 */
	public function key() {
		return key($this->array); 
	}
	
	/**
	 * @return Kafka_Message 
	 */
	public function current() {
		return current($this->array);
	}
	
	/**
	 * 
	 */
	public function rewind() {
		$this->valid = (FALSE !== reset($this->array)); 
	}
}
