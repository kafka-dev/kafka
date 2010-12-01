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

import kafka.etl.DateUtils.TimeGranularity;

public class KafkaETLCommons {

	final static String OFFSET_ROOT = "offsets.root";
	final static String KAFKA_ETL_TOPIC = "kafka.etl.topic";

	final static String OUTPUT_ROOT = "output.root";
	final static String PARTITION_GRANULARITY = "partition.granularity";

	final static String KAFKA_NODES = "kafka.nodes";
	final static String KAFKA_ETL_CONFIG = "kafka.etl.config";

	private static final String KAFKA_ETL_OFFSET_FIELD_DELIM = ":";

	final static String CLIENT_BUFFER_SIZE = "client.buffer.size";
	final static String CLIENT_TIMEOUT = "client.so.timeout";

	final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;
	final static int DEFAULT_TIMEOUT = 60000; // one minute
	public static final String IGNORE_ERRORS = "ignore.errors";

	/**
	 * get time granularity property
	 */
	public static TimeGranularity getGranularity(Props props) {
		return TimeGranularity.getGranularity(props
				.getProperty(PARTITION_GRANULARITY));
	}

	public static void setGranularity(Props props, String str) {
		props.setProperty(PARTITION_GRANULARITY, str);
	}

	/**
	 * get topic property
	 */
	public static String getTopic(Props props) {
		return props.getProperty(KAFKA_ETL_TOPIC);
	}

	public static void setTopic(Props props, String str) {
		props.setProperty(KAFKA_ETL_TOPIC, str);
	}

	/**
	 * get path to node configuration file
	 * 
	 */
	public static String getNodesPath(Props props) {
		return props.getProperty(KAFKA_NODES);
	}

	public static void setNodesPath(Props props, String str) {
		props.setProperty(KAFKA_NODES, str);
	}

	public static String getETLConfigPath(Props props) {
		return props.getProperty(KAFKA_ETL_CONFIG);
	}

	public static void setETLConfigPath(Props props, String str) {
		props.setProperty(KAFKA_ETL_CONFIG, str);
	}

	public static String getOffsetFieldDelim() {
		return KAFKA_ETL_OFFSET_FIELD_DELIM;
	}

	/**
	 * get kafka client buffer size
	 * @throws Exception 
	 * 
	 */
	public static int getClientBufferSize(Props props) throws Exception {
		return props.getInt(CLIENT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * get kafka client timeout
	 * @throws Exception 
	 * 
	 */
	public static int getClientTimeout(Props props) throws Exception {
		return props.getInt(CLIENT_TIMEOUT, DEFAULT_TIMEOUT);
	}

	public static String getOffsetRoot(Props props) {
		return props.getProperty(OFFSET_ROOT);
	}

	public static void setOffsetRoot(Props props, String str) {
		props.setProperty(OFFSET_ROOT, str);
	}

	public static String getOutRoot(Props props) {
		return props.getProperty(OUTPUT_ROOT);
	}

	public static void setOutRoot(Props props, String str) {
		props.setProperty(OUTPUT_ROOT, str);
	}

	public static String getOffset(String nodeId, String topic, int partition) {
		return getOffset(nodeId, topic, partition, "smallest");
	}

	public static String getOffset(String nodeId, String topic, int partition,
			long offset) {
		return getOffset(nodeId, topic, partition, Long.toString(offset));
	}

	public static String getDateName(String topic, String name) {
		return topic + "_" + name;
	}

	public static String getOffset(String nodeId, String topic, int partition,
			String offset) {
		return nodeId + KAFKA_ETL_OFFSET_FIELD_DELIM + topic
				+ KAFKA_ETL_OFFSET_FIELD_DELIM + partition
				+ KAFKA_ETL_OFFSET_FIELD_DELIM + offset;
	}

}
