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
import org.apache.hadoop.mapred.Reporter;
import kafka.etl.KafkaETLMapper;
import kafka.message.Message;

/**
 * Simple implementation of KafkaETLMapper. It assumes that 
 * input data are text timestamp (long).
 */
public class SimpleKafkaETLMapper extends KafkaETLMapper {

	@Override
	protected long getTimestamp(Message message) throws IOException {
		ByteBuffer buf = message.payload();
		
		byte[] array = new byte[buf.limit()];
		buf.get(array);
		
		String text = new String(array, "UTF8");
		return Long.valueOf(text);
	}

	@Override
	protected float getProgress() {
		return 0;
	}

	@Override
	protected Status getStatus(Message message, Reporter reporter) {
		return KafkaETLMapper.Status.OUTPUT_AND_CONTINUE;
	}

}
