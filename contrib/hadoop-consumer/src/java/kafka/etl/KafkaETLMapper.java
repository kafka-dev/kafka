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
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageSet;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import static kafka.api.OffsetRequest.*;

/**
 * KafkaETL mapper
 * 
 * input -- text in the following format node:topic:partition:offset
 * 
 * output -- there are two types of outputs 
 * 1. intermediate output to reducers:
 * key: KafkaETLKey (timestamp, partition) value: BytesWritable
 * 2. final output: offsets in the following format node:topic:partition:offset
 * 
 */
@SuppressWarnings("deprecation")
public abstract class KafkaETLMapper implements
		Mapper<LongWritable, Text, KafkaETLKey, BytesWritable> {

	static protected int MAX_RETRY_TIME = 1;

	protected Props _props;
	protected int _bufferSize;
	protected int _soTimeout;

	protected Map<Integer, URI> _nodes;
	protected int _partition;
	protected int _nodeId;
	protected String _topic;
	protected SimpleConsumer _consumer;

	protected MultipleOutputs _mos;
	protected OutputCollector<Text, Text> _offsetOut = null;

	protected long[] _offsetRange;
	protected long _startOffset;
	protected long _offset;
	protected boolean _toContinue = true;
	protected int _retry;
	protected long _timestamp;
	protected long _count;

	protected boolean _ignoreErrors = false;

	protected DateUtils.TimeGranularity _granularity;

	public static enum Status {
		OUTPUT_AND_CONTINUE, OUTPUT_AND_BREAK, CONTINUE, BREAK
	};

	/**
	 * Called by the default implementation of {@link #map} to reset members
	 * based on input.
	 */
	protected void reset(String input) throws IOException {
		// read topic and current offset from input
		String[] pieces = input.trim().split(
				KafkaETLCommons.getOffsetFieldDelim());
		if (pieces.length != 4)
			throw new IOException(
					input
							+ " : input must be in the form 'node:topic:partition:offset'");

		String topic = pieces[1];
		if (!topic.equalsIgnoreCase(_topic)) {
			System.out.println("This job only accepts topic " + _topic
					+ ".  Ignore topic:" + topic);
			return;
		}

		_partition = Integer.valueOf(pieces[2]);

		_nodeId = Integer.valueOf(pieces[0]);
		if (!_nodes.containsKey(_nodeId))
			throw new IOException(
					input
							+ " : invalid node id in offset file--no host information found");
		URI node = _nodes.get(_nodeId);

		// read data from queue
		_consumer = new SimpleConsumer(node.getHost(), node.getPort(),
				_soTimeout, _bufferSize);

		// get available offset range
		_offsetRange = getOffsetRange(_consumer, topic, _partition, input);

		String offsetStr = pieces[3];
		_startOffset = getStartOffset(input, offsetStr, _offsetRange);

		System.out.println("Connected to node " + _nodeId + " at " + node
				+ " beginning reading at offset " + _startOffset
				+ " latest offset=" + _offsetRange[1]);

		_offset = _startOffset;
		_retry = 0;

		_toContinue = true;
		_timestamp = 0;

		_count = 0;
	}

	/**
	 * Called by the default implementation of {@link #map} to determine
	 * stopping condition. The default implementation is to stop when reaching
	 * the maximum offset. May be overridden with alternative logic.
	 */
	protected boolean toContinue() {
		return _toContinue && _offset < _offsetRange[1]
				&& _retry < MAX_RETRY_TIME;
	}

	/**
	 * Called by the default implementation of {@link #map} to get timestamp
	 * from message. Need to be implemented by sub-class.
	 */
	abstract protected long getTimestamp(Message message) throws IOException;

	/**
	 * Called by the default implementation of {@link #map} to get percentage of
	 * completeness (to be shown in job tracker). Need to be implemented by
	 * sub-class.
	 */
	abstract protected float getProgress();

	/**
	 * Called by the default implementation of {@link #map} to determine whether
	 * to output message and continue. Accepts following status:
	 * OUTPUT_AND_CONTINUE, OUTPUT_AND_BREAK, CONTINUE, BREAK Need to be
	 * implemented by sub-class.
	 */
	abstract protected Status getStatus(Message message, Reporter reporter);

	@Override
	@SuppressWarnings("unchecked")
	public void map(LongWritable key, Text val,
			OutputCollector<KafkaETLKey, BytesWritable> collector,
			Reporter reporter) throws IOException {

		String input = val.toString();
		System.out.println("input=" + input);

		reset(input);

		long startTime = System.currentTimeMillis();

		long requestTime = 0, tempTime = 0;
		long decodeTime = 0; // , tempDecodeTime = 0;
		long outputTime = 0; // , tempOutputTime = 0;

		while (toContinue()) {

			// fetch data using multi-fetch interfaces
			// TODO: change to high-level interface once it supports "reset"
			FetchRequest fetchRequest = new FetchRequest(_topic, _partition,
					_offset, _bufferSize);

			List<FetchRequest> array = new ArrayList<FetchRequest>();
			array.add(fetchRequest);

			tempTime = System.currentTimeMillis();
			MultiFetchResponse response = _consumer.multifetch(array);
			requestTime += (System.currentTimeMillis() - tempTime);

			while (response.hasNext()) {
				ByteBufferMessageSet messages = response.next();

				// check error codes
				_toContinue = checkErrorCode(messages, input);
				if (!_toContinue)
					break;

				Iterator<Message> iter = (Iterator<Message>) messages
						.iterator();
				long messageOffset = 0;
				while (iter.hasNext()) {
					Message message = iter.next();

					messageOffset += MessageSet.entrySize(message);
					reporter.incrCounter("topic-counters", _topic, 1);
					_count++;

					try {
						tempTime = System.currentTimeMillis();
						_timestamp = getTimestamp(message);
						decodeTime += (System.currentTimeMillis() - tempTime);

					} catch (IOException e) {
						System.err.println("SetOffset=" + _offset
								+ "messageOffset=" + messageOffset
								+ ": ignore message with exception: ");

						if (_ignoreErrors) {
							reporter.incrCounter(_topic, _topic
									+ "_PARSING_ERROR", 1);
							continue;
						} else {
							e.printStackTrace(System.err);
							throw e;
						}
					}

					// determine whether to stop
					Status status = getStatus(message, reporter);

					// generate output
					switch (status) {
					case OUTPUT_AND_CONTINUE:
					case OUTPUT_AND_BREAK:
						tempTime = System.currentTimeMillis();
						ByteBuffer buffer = message.payload();
						byte[] bytes = new byte[buffer.remaining()];
						buffer.get(bytes, buffer.position(), bytes.length);
						collector.collect(new KafkaETLKey(_timestamp,
								_granularity), new BytesWritable(bytes));
						outputTime += (System.currentTimeMillis() - tempTime);

					}

					// report progress
					float percentage = getProgress();
					reporter.setStatus("collected " + percentage + "%");

					switch (status) {
					case OUTPUT_AND_BREAK:
					case BREAK:
						break;
					}

				}

				_offset += messages.validBytes();
			}
		}
		_consumer.close();
		long endTime = System.currentTimeMillis();

		// output offsets
		// Note: name can only contain chars and numbers
		String offsetName = _nodeId + _topic + _partition;
		Text offsetText = new Text(KafkaETLCommons.getOffset(
				Integer.toString(_nodeId), _topic, _partition, _offset));

		if (_offsetOut == null)
			_offsetOut = _mos.getCollector("offset", offsetName, reporter);
		_offsetOut.collect(null, offsetText);

		// now record some stats
		double secondsEllapsed = (endTime - startTime) / 1000.0;
		reporter.incrCounter(_topic, "total-time(ms)",    (endTime - startTime) );
		reporter.incrCounter(_topic, "request-time(ms)", requestTime);
		reporter.incrCounter(_topic, "decode-time(ms)", decodeTime);
		reporter.incrCounter(_topic, "output-time(ms)", outputTime);

		long bytesRead = _offset - _startOffset;
		reporter.incrCounter(_topic, "bytes-read", (long) bytesRead);

		double bytesPerSec = bytesRead / secondsEllapsed;
		NumberFormat format = NumberFormat.getInstance();
		format.setMaximumFractionDigits(2);
		System.out.println("Completed reading at offset " + _offset
				+ " for node " + _nodeId + " on "
				+ _nodes.get(_nodeId).toString());
		double megabytes = bytesPerSec / (1024.0 * 1024.0);
		System.out.println("Read " + bytesRead + " bytes in "
				+ ((int) secondsEllapsed) + " seconds ("
				+ format.format(megabytes) + " MB/sec)");

		System.out
				.println("count\tbytesRead\ttotalTime(ms)\trequestTime(ms)\tdecodeTime(ms)\toutputTime(ms)");

		System.out.println(_count + "\t" + bytesRead + "\t"
				+ (endTime - startTime) + "\t" + requestTime + "\t"
				+ decodeTime + "\t" + outputTime);

	}

	/**
	 * Called by the default implementation of {@link #map} to check error code
	 * to determine whether to continue.
	 */
	protected boolean checkErrorCode(ByteBufferMessageSet messages, String input)
			throws IOException {
		int errorCode = messages.errorCOde();
		if (errorCode == ErrorMapping.OFFSET_OUT_OF_RANGE_CODE()) {
			// offset cannot cross the maximum offset (guaranteed by Kafka
			// protocol)
			// Kafka server may delete old files from time to time
			System.err.println("WARNING: current offset=" + _offset
					+ ". It is out of range.");

			if (_retry >= MAX_RETRY_TIME)
				return false;

			_retry++;
			// get the current offset range
			_offsetRange = getOffsetRange(_consumer, _topic, _partition, input);
			_offset = _startOffset = _offsetRange[0];
			return true;

		} else if (errorCode == ErrorMapping.INVALID_MESSAGE_CODE()) {
			throw new IOException(input + " current offset=" + _offset
					+ " : invalid offset.");
		} else if (errorCode == ErrorMapping.WRONG_PARTITION_CODE()) {
			throw new IOException(input + " : wrong partition");
		} else if (errorCode != ErrorMapping.NO_ERROR()) {
			throw new IOException(input + " current offset=" + _offset
					+ " error:" + errorCode);
		} else
			return true;
	}

	/**
	 * Get offset ranges
	 * 
	 */
	protected long[] getOffsetRange(SimpleConsumer consumer, String topic,
			int partition, String input) throws IOException {

		long[] range = new long[2];

		long[] offsets = consumer.getOffsetsBefore(topic, partition,
				OffsetRequest.EARLIEST_TIME(), 1);

		if (offsets.length != 1)
			throw new IOException("input:" + input
					+ " Expect one smallest offset");

		range[0] = offsets[0];

		offsets = consumer.getOffsetsBefore(topic, partition,
				OffsetRequest.LATEST_TIME(), 1);

		if (offsets.length != 1)
			throw new IOException("input:" + input
					+ " Expect one latest offset");
		range[1] = offsets[0];
		return range;
	}

	/**
	 * Get start offset
	 */
	protected long getStartOffset(String input, String offsetStr, long[] range)
			throws IOException {

		if (offsetStr.equalsIgnoreCase("smallest")) {
			return range[0];
		} else {
			Long start = Long.parseLong(offsetStr);
			if (start > range[1]) {
				throw new IOException("input:" + input
						+ " start offset is larger than latest one:" + range[1]);

			} else if (start < range[0]) {
				System.err.println("WARNING: input:" + input
						+ " start_offset is smaller than smallest_offset:"
						+ range[0] + ". Will use the samllest instead.");
				return range[0];
			} else {
				return start;
			}
		}
	}

	@Override
	public void close() throws IOException {
		_mos.close();
	}

	@Override
	public void configure(JobConf conf) {
		try {

			_props = KafkaETLUtils.getPropsFromJob(conf);
			String nodePath = KafkaETLCommons.getNodesPath(_props);
			Props nodesProps = KafkaETLUtils.readProps(nodePath);
			System.out.println(nodesProps);
			_nodes = new HashMap<Integer, URI>();
			for (String key : nodesProps.stringPropertyNames()) {
				_nodes.put(Integer.parseInt(key),nodesProps.getUri(key));
			}

			_bufferSize = KafkaETLCommons.getClientBufferSize(_props);
			_soTimeout = KafkaETLCommons.getClientTimeout(_props);

			System.out.println("bufferSize=" + _bufferSize);
			System.out.println("timeout=" + _soTimeout);

			_granularity = KafkaETLCommons.getGranularity(_props);

			_topic = KafkaETLCommons.getTopic(_props);
			System.out.println("topic=" + _topic);

			_mos = new MultipleOutputs(conf);

			_ignoreErrors = _props.getBoolean(KafkaETLCommons.IGNORE_ERRORS,
					false);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
