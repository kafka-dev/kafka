/*
 * producer.hpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_PRODUCER_HPP_
#define KAFKA_PRODUCER_HPP_

#include <string>
#include <vector>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <stdint.h>

#include "encoder.hpp"

namespace kafkaconnect {

const uint32_t use_random_partition = 0xFFFFFFFF;

class producer
{
public:
	producer(boost::asio::io_service& io_service);
	~producer();

	void connect(const std::string& hostname, const uint16_t port);
	void connect(const std::string& hostname, const std::string& servicename);

	void close();
	bool is_connected() const;

	bool send(const std::string& message, const std::string& topic, const uint32_t partition = kafkaconnect::use_random_partition)
	{
		boost::array<std::string, 1> messages = { { message } };
		return send(messages, topic, partition);
	}

	// TODO: replace this with a sending of the buffered data so encode is called prior to send this will allow for decoupling from the encoder
	template <typename List>
	bool send(const List& messages, const std::string& topic, const uint32_t partition = kafkaconnect::use_random_partition)
	{
		if (!is_connected())
		{
			return false;
		}

		// TODO: make this more efficient with memory allocations.
		boost::asio::streambuf* buffer = new boost::asio::streambuf();
		std::ostream stream(buffer);

		kafkaconnect::encode(stream, topic, partition, messages);

		boost::asio::async_write(
			_socket, *buffer,
			boost::bind(&producer::handle_write_request, this, boost::asio::placeholders::error, buffer)
		);

		return true;
	}


private:
	bool _connected;
	boost::asio::ip::tcp::resolver _resolver;
	boost::asio::ip::tcp::socket _socket;

	void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_write_request(const boost::system::error_code& error_code, boost::asio::streambuf* buffer);
};

}

#endif /* KAFKA_PRODUCER_HPP_ */
