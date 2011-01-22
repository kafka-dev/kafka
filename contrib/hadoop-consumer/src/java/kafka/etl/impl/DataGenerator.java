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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import kafka.etl.KafkaETLKey;
import kafka.etl.KafkaETLRequest;
import kafka.etl.KafkaETLUtils;
import kafka.etl.Props;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.SimpleProducer;

/**
 * Use this class to produce test events to Kafka server. Each event contains a
 * random timestamp in text format.
 */
@SuppressWarnings("deprecation")
public class DataGenerator {

	protected final static Random RANDOM = new Random(
			System.currentTimeMillis());

	protected Props _props;
	protected SimpleProducer _producer = null;
	protected URI _uri = null;
	protected String _topic;
	protected int _count;
	protected String _offsetsDir;
	protected final int TCP_BUFFER_SIZE = 300 * 1000;
	protected final int CONNECT_TIMEOUT = 20000; // ms
	protected final int RECONNECT_INTERVAL = Integer.MAX_VALUE; // ms

	public DataGenerator(String id, Props props) throws Exception {
		_props = props;
		_topic = props.getProperty("kafka.etl.topic");
		System.out.println("topics=" + _topic);
		_count = props.getInt("event.count");

		_offsetsDir = _props.getProperty("input");
		
		// initialize kafka producer to generate count events
		String serverUri = _props.getProperty("kafka.server.uri");
		_uri = new URI (serverUri);
		
		System.out.println("server uri:" + _uri.toString());
		_producer = new SimpleProducer(_uri.getHost(), _uri.getPort(),
					TCP_BUFFER_SIZE, CONNECT_TIMEOUT, RECONNECT_INTERVAL);
			
	}

	public void run() throws Exception {

		List<Message> list = new ArrayList<Message>();
		for (int i = 0; i < _count; i++) {
			Long timestamp = RANDOM.nextLong();
			if (timestamp < 0) timestamp = -timestamp;
			byte[] bytes = timestamp.toString().getBytes("UTF8");
			Message message = new Message(bytes);
			list.add(message);
		}
		// send events
		System.out.println(" send " + list.size() + " " + _topic + " count events to " + _uri);
		_producer.send(_topic, new ByteBufferMessageSet(list));

		// close the producer
		_producer.close();
		
		// generate offset files
		generateOffsets();
	}

    protected void generateOffsets() throws Exception {
        JobConf conf = new JobConf();
        conf.set("hadoop.job.ugi", _props.getProperty("hadoop.job.ugi"));
        
        Path outPath = new Path(_offsetsDir + Path.SEPARATOR + "1.dat");
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) fs.delete(outPath);
        
        KafkaETLRequest request =
            new KafkaETLRequest(_topic, "tcp://" + _uri.getHost() + ":" + _uri.getPort(), 0);

        System.out.println("Dump " + request.toString() + " to " + outPath.toUri().toString());
        byte[] bytes = request.toString().getBytes("UTF-8");
        KafkaETLKey dummyKey = new KafkaETLKey();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outPath, 
                                        KafkaETLKey.class, BytesWritable.class);
        writer.append(dummyKey, new BytesWritable(bytes));
        writer.close();
    }
    
	public static void main(String[] args) throws Exception {

		if (args.length < 1)
			throw new Exception("Usage: - config_file");

		Props props = new Props(args[0]);
		DataGenerator job = new DataGenerator("DataGenerator", props);
		job.run();
	}

}
