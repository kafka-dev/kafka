<?php

/**
 * Description of SimpleConsumer
 *
 * @author Lorenzo Alberton <l.alberton@quipo.it>
 */
class Kafka_SimpleConsumer {
	
	protected $host             = 'localhost';
	protected $port             = 9092;
	protected $socketTimeout    = 10;
	protected $socketBufferSize = 1000000;

	/**
	 * @var resource
	 */
	protected $conn = null;
	
	/**
	 * @param integer $host
	 * @param integer $port
	 * @param integer $socketTimeout
	 * @param integer $socketBufferSize 
	 */
	public function __construct($host, $port, $socketTimeout, $socketBufferSize) {
		$this->host = $host;
		$this->port = $port;
		$this->socketTimeout    = $socketTimeout;
		$this->socketBufferSize = $socketBufferSize;
	}
	
	/**
	 * 
	 */
	public function connect() {
		if (!is_resource($this->conn)) {
			$this->conn = stream_socket_client('tcp://' . $this->host . ':' . $this->port, $errno, $errstr);
			if (!$this->conn) {
				throw new RuntimeException($errstr, $errno);
			}
			stream_set_timeout($this->conn,      $this->socketTimeout);
			stream_set_read_buffer($this->conn,  $this->socketBufferSize);
			stream_set_write_buffer($this->conn, $this->socketBufferSize);
			//echo "\nConnected to ".$this->host.":".$this->port."\n";
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
	 * @param Kafka_FetchRequest $req
	 *
	 * @return Kafka_MessageSet $messages
	 */
	public function fetch(Kafka_FetchRequest $req) {
		$this->connect();
		$this->sendRequest($req);
		//echo "\nRequest sent: ".(string)$req."\n";
		$response = $this->getResponse();
		//var_dump($response);
		$this->close();
		return new Kafka_MessageSet($response['response']->buffer, $response['errorCode']);
	}
	
	/**
	 * @param Kafka_FetchRequest $req 
	 */
	protected function sendRequest(Kafka_FetchRequest $req) {
		$send = new Kafka_BoundedByteBuffer_Send($req);
		$send->writeCompletely($this->conn);
	}
	
	/**
	 * @return array
	 */
	protected function getResponse() {
		$response = new Kafka_BoundedByteBuffer_Receive();
		$response->readCompletely($this->conn);
		
		rewind($response->buffer);
		// this has the side effect of setting the initial position of buffer correctly
		$errorCode = array_shift(unpack('n', fread($response->buffer, 2))); 
		//rewind($response->buffer);
		return array(
			'response'  => $response, 
			'errorCode' => $errorCode,
		);
	}
	
}
