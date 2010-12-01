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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import kafka.etl.DateUtils.TimeGranularity;

/**
 * Map output key which contains timestamp and partition determined by time
 * granularity.
 * 
 */
public class KafkaETLKey implements WritableComparable<KafkaETLKey> {

	private long partition;
	private long timestamp;

	public KafkaETLKey() {
		set(0, 0);
	}

	public KafkaETLKey(long timestamp, TimeGranularity granularity) {
		long partition = KafkaETLUtils.getPartition(timestamp, granularity);
		set(partition, timestamp);
	}

	public void set(long partition, long timestamp) {
		this.partition = partition;
		this.timestamp = timestamp;
	}

	public long getPartition() {
		return partition;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		partition = in.readLong();
		timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(partition);
		out.writeLong(timestamp);
	}

	@Override
	public int compareTo(KafkaETLKey pair) {
		long cmp = partition - pair.getPartition();
		if (cmp == 0) {
			cmp = timestamp - pair.getTimestamp();
			return toInt(cmp);
		} else
			return toInt(cmp);
	}

	public static int toInt(long ret) {
		if (ret > 0)
			return 1;
		else if (ret < 0)
			return -1;
		else
			return 0;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (!(o instanceof KafkaETLKey))
			return false;

		return compareTo((KafkaETLKey) o) == 0;
	}

	@Override
	public int hashCode() {
		return (new Long(partition)).hashCode();
	}

	@Override
	public String toString() {
		return partition + "\t" + timestamp;
	}

}
