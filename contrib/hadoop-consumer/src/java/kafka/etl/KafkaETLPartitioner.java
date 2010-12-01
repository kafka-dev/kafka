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

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Partitioner that partitions based on time granularity
 */
@SuppressWarnings("deprecation")
public class KafkaETLPartitioner implements
		Partitioner<KafkaETLKey, BytesWritable> {

	@Override
	public int getPartition(KafkaETLKey key, BytesWritable value,
			int numPartitions) {
		return (int) (key.getPartition() % numPartitions);
	}

	@Override
	public void configure(JobConf conf) {
	}

}