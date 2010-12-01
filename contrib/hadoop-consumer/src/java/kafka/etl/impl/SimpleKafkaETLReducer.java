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
import java.nio.ByteBuffer;
import kafka.etl.KafkaETLKey;
import kafka.etl.KafkaETLReducer;
import kafka.message.Message;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Simple implementation of KafkaETLReducer. It assumes that 
 * input data are text timestamp (long).
 */
public class SimpleKafkaETLReducer extends KafkaETLReducer<NullWritable, Text> {

	final static Text dummyKey = new Text ("key");
	
	@Override
	protected NullWritable generateOutputKey(KafkaETLKey key, Message message) 
	throws IOException {
		return NullWritable.get();
	}

	@Override
	protected Text generateOutputValue(KafkaETLKey key, Message message) 
	throws IOException {
		ByteBuffer buf = message.payload();
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		
		String text = new String(array, "UTF8");
		return new Text(text);
	}
}
