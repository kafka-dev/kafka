/*
 * encoder_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafkaconnect
#include <boost/test/unit_test.hpp>

#include <string>
#include <vector>

#include "../encoder.hpp"

BOOST_AUTO_TEST_CASE(single_message_test)
{
	std::ostringstream stream;

	std::vector<std::string> messages;
	messages.push_back("test message");

	kafkaconnect::encode(stream, "topic", 1, messages);

	BOOST_CHECK_EQUAL(stream.str().length(), 4 + 2 + 2 + strlen("topic") + 4 + 4 + 9 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(3), 2 + 2 + strlen("topic") + 4 + 4 + 9 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(5), 0);
	BOOST_CHECK_EQUAL(stream.str().at(7), strlen("topic"));
	BOOST_CHECK_EQUAL(stream.str().at(8), 't');
	BOOST_CHECK_EQUAL(stream.str().at(8 + strlen("topic") - 1), 'c');
	BOOST_CHECK_EQUAL(stream.str().at(11 + strlen("topic")), 1);
	BOOST_CHECK_EQUAL(stream.str().at(15 + strlen("topic")), 9 + strlen("test message"));
	BOOST_CHECK_EQUAL(stream.str().at(16 + strlen("topic")), 0);
	BOOST_CHECK_EQUAL(stream.str().at(25 + strlen("topic")), 't');
}

BOOST_AUTO_TEST_CASE(multiple_message_test)
{
	std::ostringstream stream;

	std::vector<std::string> messages;
	messages.push_back("test message");
	messages.push_back("another message to check");

	kafkaconnect::encode(stream, "topic", 1, messages);

	BOOST_CHECK_EQUAL(stream.str().length(), 4 + 2 + 2 + strlen("topic") + 4 + 4 + 9 + strlen("test message") + 9 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(3), 2 + 2 + strlen("topic") + 4 + 4 + 9 + strlen("test message") + 9 + strlen("another message to check"));
	BOOST_CHECK_EQUAL(stream.str().at(15 + strlen("topic")), 9 + strlen("test message") + 9 + strlen("another message to check"));
}

