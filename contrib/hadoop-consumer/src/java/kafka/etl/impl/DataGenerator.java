/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.etl.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import kafka.etl.KafkaETLCommons;
import kafka.etl.KafkaETLUtils;
import kafka.etl.Props;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.SimpleProducer;

/**
 * Use this class to produce test events to Kafka server. Each event contains a
 * random timestamp in text format.
 */
public class DataGenerator {

	protected final static Random RANDOM = new Random(
			System.currentTimeMillis());

	protected Props _props;
	protected List<SimpleProducer> _producers = null;
	protected String _topic;
	protected int _count;
	protected final int TCP_BUFFER_SIZE = 300 * 1000;
	protected final int CONNECT_TIMEOUT = 20000; // ms
	protected final int RECONNECT_INTERVAL = Integer.MAX_VALUE; // ms

	public DataGenerator(String id, Props props) throws Exception {
		_props = props;
		_topic = props.getProperty("kafka.etl.topic");
		System.out.println("topics=" + _topic);
		_count = props.getInt("event.count");

		// initialize kafka producer to generate count events
		String nodePath = KafkaETLCommons.getNodesPath(_props);
		System.out.println("node path=" + nodePath);
		Props nodesProps = KafkaETLUtils.readProps(nodePath);
		_producers = new ArrayList<SimpleProducer>();
		for (String key : nodesProps.stringPropertyNames()) {
			URI uri = nodesProps.getUri(key);
			System.out.println("server uri:" + uri.toString());
			_producers.add(new SimpleProducer(uri.getHost(), uri.getPort(),
					TCP_BUFFER_SIZE, CONNECT_TIMEOUT, RECONNECT_INTERVAL));
		}
	}

	public void run() throws IOException, URISyntaxException {

		int producerId = RANDOM.nextInt() % _producers.size();
		SimpleProducer producer = _producers.get(producerId);

		List<Message> list = new ArrayList<Message>();
		for (int i = 0; i < _count; i++) {
			Long timestamp = RANDOM.nextLong();
			if (timestamp < 0) timestamp = -timestamp;
			byte[] bytes = timestamp.toString().getBytes("UTF8");
			Message message = new Message(bytes);
			list.add(message);
		}
		// send events
		System.out.println(" send " + list.size() + " " + _topic
				+ " count events to " + producerId);
		producer.send(_topic, new ByteBufferMessageSet(list));

		// close all producers
		for (SimpleProducer p : _producers) {
			p.close();
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1)
			throw new Exception("Usage: - config_file");

		Props props = new Props(args[0]);
		DataGenerator job = new DataGenerator("DataGenerator", props);
		job.run();
	}

}
