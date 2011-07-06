/*
 * producer.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#include <boost/lexical_cast.hpp>

#include "producer.hpp"

namespace kafkaconnect {

producer::producer(boost::asio::io_service& io_service, const error_handler_function& error_handler)
	: _connected(false)
	, _resolver(io_service)
	, _socket(io_service)
	, _error_handler(error_handler)
{
}

producer::~producer()
{
	close();
}

void producer::connect(const std::string& hostname, const uint16_t port)
{
	connect(hostname, boost::lexical_cast<std::string>(port));
}

void producer::connect(const std::string& hostname, const std::string& servicename)
{
	boost::asio::ip::tcp::resolver::query query(hostname, servicename);
	_resolver.async_resolve(
		query,
		boost::bind(
			&producer::handle_resolve, this,
			boost::asio::placeholders::error, boost::asio::placeholders::iterator
		)
	);
}

void producer::close()
{
	_connected = false;
	_socket.close();
}

bool producer::is_connected() const
{
	return _connected;
}


void producer::handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		boost::asio::ip::tcp::endpoint endpoint = *endpoints;
		_socket.async_connect(
			endpoint,
			boost::bind(
				&producer::handle_connect, this,
				boost::asio::placeholders::error, ++endpoints
			)
		);
	}
	else { fail_fast_error_handler(error_code); }
}

void producer::handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		// The connection was successful. Send the request.
		_connected = true;
	}
	else if (endpoints != boost::asio::ip::tcp::resolver::iterator())
	{
		// TODO: handle connection error (we might not need this as we have others though?)

		// The connection failed, but we have more potential endpoints so throw it back to handle resolve
		_socket.close();
		handle_resolve(boost::system::error_code(), endpoints);
	}
	else { fail_fast_error_handler(error_code); }
}

void producer::handle_write_request(const boost::system::error_code& error_code, boost::asio::streambuf* buffer)
{
	if (error_code)
	{
		fail_fast_error_handler(error_code);
	}

	delete buffer;
}

}
