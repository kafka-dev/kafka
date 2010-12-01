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

package kafka.etl;

import java.io.IOException;
import java.util.Iterator;

import kafka.message.Message;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * KafkaETL reducer is used to aggregate data based on time granularity.
 * 
 * input -- key: KafkaETLKey (timestamp, partition) value: BytesWritable
 * 
 */
@SuppressWarnings("deprecation")
public abstract class KafkaETLReducer<K, V> implements
		Reducer<KafkaETLKey, BytesWritable, K, V> {

	protected Props _props;
	protected String _topic;
	protected DateUtils.TimeGranularity _granularity;

	@Override
	public void reduce(KafkaETLKey key, Iterator<BytesWritable> values,
			OutputCollector<K, V> collector, Reporter reporter)
			throws IOException {

		reset(key, collector, reporter);

		while (values.hasNext()) {
			byte[] bytes = values.next().get();

			Message message = new Message(bytes);

			if (!filter(key, message, reporter))
				collector.collect(generateOutputKey(key, message),
						generateOutputValue(key, message));

			process(key, message, collector, reporter);
		}

		processGroup(key, collector, reporter);
	}

	/**
	 * Called by the default implementation of {@link #reduce} to reset members
	 * based on input key. The default implementation does nothing.
	 */
	protected void reset(KafkaETLKey key, OutputCollector<K, V> collector,
			Reporter reporter) throws IOException{
	}

	/**
	 * Called by the default implementation of {@link #reduce} to filter values.
	 * The default implementation filters nothing.
	 */
	protected boolean filter(KafkaETLKey key, Message message, Reporter reporter)
	throws IOException{
		return false;
	}

	/**
	 * Called by the default implementation of {@link #reduce} to process (key,
	 * value) pair. The default implementation does nothing.
	 */
	protected void process(KafkaETLKey key, Message message,
			OutputCollector<K, V> collector, Reporter reporter) 
	throws IOException{
	}

	/**
	 * Called by the default implementation of {@link #reduce} to process after
	 * iterating all values. The default implementation does nothing.
	 */
	protected void processGroup(KafkaETLKey key,
			OutputCollector<K, V> collector, Reporter reporter)
			throws IOException {
	}

	/**
	 * Called by the default implementation of {@link #reduce} to generate
	 * output key. Need to be implemented by sub-class.
	 */
	abstract protected K generateOutputKey(KafkaETLKey key, Message message)
	throws IOException ;

	/**
	 * Called by the default implementation of {@link #reduce} to generate
	 * output value. Need to be implemented by sub-class.
	 */
	abstract protected V generateOutputValue(KafkaETLKey key, Message message)
	throws IOException ;

	@Override
	public void configure(JobConf conf) {
		_props = KafkaETLUtils.getPropsFromJob(conf);
		_topic = KafkaETLCommons.getTopic(_props);
		_granularity = KafkaETLCommons.getGranularity(_props);
	}

	@Override
	public void close() throws IOException {
	}

}
