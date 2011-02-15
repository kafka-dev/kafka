<?php

/**
 * Description of Kafka_Producer
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_Producer {
	/**
	 * @var integer
	 */
	protected $request_key;

	/**
	 * @var resource
	 */
	protected $conn;
	
	/**
	 * @var string
	 */
	protected $host;
	
	/**
	 * @var integer
	 */
	protected $port;

	/**
	 * @param integer $host
	 * @param integer $port
	 */
	public function __construct($host, $port) {
		$this->request_key = 0;
		$this->host = $host;
		$this->port = $port;
	}
	
	public function connect() {
		if (!is_resource($this->conn)) {
			$this->conn = stream_socket_client('tcp://' . $this->host . ':' . $this->port);
		}
	}

	/**
	 *
	 */
	public function close() {
		if (is_resource($this->conn)) {
			fclose($this->conn);
		}
	}

	/**
	 * @param array   $messages
	 * @param string  $topic
	 * @param integer $partition
	 *
	 * @return boolean
	 */
	public function send(array $messages, $topic, $partition = 0) {
		$this->connect();
		return fwrite($this->conn, Kafka_Encoder::encode_produce_request($topic, $partition, $messages));
	}
}
