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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.joda.time.format.DateTimeFormatter;


@SuppressWarnings("deprecation")
public abstract class KafkaETLJob {

	public static final String HADOOP_PREFIX = "hadoop-conf.";
	protected static final String KAFKA_ETL_CONFIG_FIELD_DELIM = " ";
	protected static final String KAFKA_ETL_CONFIG_PARTITION_RANGES_DELIM = ",";
	protected static final String KAFKA_ETL_CONFIG_PARTITION_RANGE_STARTEND_DELIM = "-";

	protected static final DateTimeFormatter timeFormatter = DateUtils
			.getDateTimeFormatter("YYYYMMdd-HHmmss");

	private String _topic;
	private String _inputRoot; // input offsets location
	private String _outputRoot; // output data location
	private String _configPath;
	private String _nodesPath;
	private long _timestamp; // current timestamp

	private Props _props = null;
	private String _name = null;
	private RunningJob _runningJob = null;
	private JobConf _conf = null;
	private Path _outputPath = null;
	private Path _inputPath = null;
	private FileSystem _fs = null;

	/**
	 * constructor
	 * 
	 * @param	name		job name
	 * @param props		job properties
	 */
	public KafkaETLJob(String name, Props props) throws Exception {
		_props = props;
		_name = name;

		_props = props;
		_topic = KafkaETLCommons.getTopic(props);
		_inputRoot = KafkaETLCommons.getOffsetRoot(props);
		_outputRoot = KafkaETLCommons.getOutRoot(props);
		_timestamp = System.currentTimeMillis();

		System.out.println("time=" + _timestamp + " "
				+ DateUtils.print(_timestamp));

		_nodesPath = KafkaETLCommons.getNodesPath(props);
		_configPath = KafkaETLCommons.getETLConfigPath(props);

		_conf = getJobConf();
		_inputPath = getInputPath();
		_outputPath = getOutputPath();
		_fs = getFileSystem();
	}

	/**
	 * Get JobConfiguration. Initialize it if not existing.
	 * @return
	 * @throws IOException
	 */
	public JobConf getJobConf() throws Exception {
		if (_conf == null)
			_conf = initializeJobConf();
		return _conf;
	}

	/**
	 * Get job properties
	 * @return
	 */
	public Props getProps() {
		return _props;
	}
	
	/**
	 * Get topic name
	 * @return
	 */
	public String getTopic() {
		return _topic;
	}

	/**
	 * Get output path. Initialize it if not existing.
	 * @return
	 * @throws IOException
	 */
	public Path getOutputPath() throws Exception {
		if (_outputPath == null)
			_outputPath = generateOutputPath();
		return _outputPath;
	}

	/**
	 * Get input path. Initialize it if not existing.
	 * @return
	 * @throws IOException
	 */
	public Path getInputPath() throws Exception {
		if (_inputPath == null)
			_inputPath = generateInputPath();
		return _inputPath;
	}

	/**
	 * Get file system. Initialize it if not existing.
	 * @return
	 * @throws IOException
	 */
	protected FileSystem getFileSystem() throws Exception {
		if (_fs == null) {
			JobConf conf = getJobConf();
			_fs = FileSystem.get(conf);
			;
		}
		return _fs;
	}

	/**
	 * Get current timestamp
	 * @return
	 */
	protected long getTimestamp() {
		return _timestamp;
	}
	
	/**
	 * Get job id
	 * @return
	 */
	protected String getJobId() {
		return _name;
	}

	/**
	 * Get mapper class
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	abstract protected Class getMapperClass();

	/**
	 * Get reducer class
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	abstract protected Class getReducerClass();

	/**
	 * Get the number of reducers
	 * @return
	 * @throws IOException
	 */
	abstract protected int getNumReducers() throws Exception;

	/**
	 * Get running job
	 * @return
	 */
	protected RunningJob getRunningJob() {
		return _runningJob;
	}

	/**
	 * Create a job configuration
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	protected JobConf createJobConf() throws Exception {
		_conf = getJobConf("kafkaETL");
		_conf.setJobName(getJobId());
		_conf.setMapperClass(getMapperClass());
		_conf.setReducerClass(getReducerClass());

		_conf.setJarByClass(this.getClass());

		_conf.setMapOutputKeyClass(KafkaETLKey.class);
		_conf.setMapOutputValueClass(BytesWritable.class);
		_conf.setMapSpeculativeExecution(false);
		_conf.setCompressMapOutput(false);

		_conf.setPartitionerClass(KafkaETLPartitioner.class);
		_conf.setOutputKeyComparatorClass(KafkaETLKeyComparator.class);
		_conf.setOutputValueGroupingComparator(KafkaETLGroupingComparator.class);

		_conf.setReduceSpeculativeExecution(false);
		_conf.setNumReduceTasks(getNumReducers());

		TextInputFormat.setInputPaths(_conf, _inputPath);
		_conf.setInputFormat(TextInputFormat.class);

		if (_fs.exists(_outputPath))
			_fs.delete(_outputPath, true);
		FileOutputFormat.setOutputPath(_conf, _outputPath);

		// offset output formats
		MultipleOutputs.addMultiNamedOutput(_conf, "offset",
				TextOutputFormat.class, Text.class, Text.class);
		// data output formats
		_conf.setOutputFormat(SequenceFileOutputFormat.class);
		_conf.setOutputKeyClass(Text.class);
		_conf.setOutputValueClass(BytesWritable.class);
		FileOutputFormat.setCompressOutput(_conf, true);

		return _conf;
	}

	/**
	 * Run the job
	 * @throws Exception
	 */
	public void run() throws Exception {

		runHadoopJob();
		boolean ret = _runningJob.isSuccessful();
		if (ret) {
			postProcessing();
		} else {
			throw new Exception("Hadoop job " + getJobId() + " failed!");
		}
	}

	/**
	 * Run hadoop job
	 * @throws Exception
	 */
	protected void runHadoopJob() throws Exception {
		_conf = createJobConf();
		_runningJob = new JobClient(_conf).submitJob(_conf);

		String id = _runningJob.getJobID();
		System.out.println("Hadoop job id=" + id);
		_runningJob.waitForCompletion();

	}

	/**
	 * Post Hadoop job
	 * @throws Exception
	 */
	protected void postProcessing() throws Exception {

	}

	/**
	 * Initialize job configuration and set hadoop.job.ugi
	 * @return
	 * @throws IOException
	 */
	protected JobConf initializeJobConf() throws Exception {
		JobConf conf = new JobConf();
		conf.set("hadoop.job.ugi", _props.getProperty("hadoop.job.ugi"));
		return conf;
	}

	/**
	 * Generate output path wrt the topic
	 * @return
	 * @throws IOException
	 */
	protected Path generateOutputPath() throws Exception {
		String output = _outputRoot + Path.SEPARATOR + _topic;
		return createOutputDir(getFileSystem(), output);
	}

	/**
	 * Generate input path. If the directory (wrt the topic) doesn't
	 * exist, generate default offsets which start from smallest
	 * offsets
	 * 
	 * @return
	 * @throws IOException
	 */
	protected Path generateInputPath() throws Exception {

		FileSystem fs = getFileSystem();
		Path inputPathRoot = new Path(_inputRoot + Path.SEPARATOR + _topic);

		Path inputPath = null;

		if (fs.exists(inputPathRoot)) {
			FileStatus[] statuses = fs.listStatus(inputPathRoot,
					KafkaETLUtils.PATH_FILTER);
			if (statuses.length > 0) {
				Arrays.sort(statuses);
				// ignore the empty directories
				for (int i = statuses.length - 1; i >= 0; i--) {
					Path path = statuses[i].getPath();
					FileStatus[] innerStatuses = fs.listStatus(path,
							KafkaETLUtils.PATH_FILTER);
					if (innerStatuses.length > 0) {
						inputPath = path;
						break;
					}
				}
			}
		}

		if (inputPath == null)
			inputPath = generateStartOffsetFiles(fs, inputPathRoot);

		return inputPath;
	}

	/**
	 * generate start offset files
	 */
	protected Path generateStartOffsetFiles(FileSystem fs, Path root)
	throws Exception {

		/* this is to make sure it is different than the output offset dir */
		Path offsetOut = createOutputDir(fs, root, _timestamp - 2000);

		Path configPath = new Path(_configPath);
		if (!fs.exists(configPath))
			throw new IOException("config file " + _configPath
					+ " does not exist!");

		Props nodesProps = KafkaETLUtils.readProps(_nodesPath);

		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(configPath)));
		String line;
		while ((line = in.readLine()) != null) {
			String[] parts = line.split(KAFKA_ETL_CONFIG_FIELD_DELIM);
			if (parts.length != 3)
				throw new IOException("Invalid line " + line
						+ ". Expect format: topicName nodeId partitionRange.");

			String topic = parts[0];
			if (!topic.equals(_topic))
				continue;

			String nodeId = parts[1].trim();
			if (!nodesProps.containsKey(nodeId))
				throw new IOException("No node for id " + nodeId
						+ " is defined.");

			String partitionRange = parts[2];
			Set<Integer> partitions = new TreeSet<Integer>();
			String[] ranges = partitionRange
					.split(KAFKA_ETL_CONFIG_PARTITION_RANGES_DELIM);
			for (String range : ranges) {
				String[] list = range
						.split(KAFKA_ETL_CONFIG_PARTITION_RANGE_STARTEND_DELIM);
				if (list.length != 2)
					throw new IOException("Invalid range " + range
							+ ". Expeact format: start-end.");
				int start = Integer.parseInt(list[0]);
				int end = Integer.parseInt(list[1]);
				for (int i = start; i <= end; i++) {
					partitions.add(i);
				}
			}

			for (Integer partition : partitions) {
				String fileName = "offset_" + nodeId + _topic + partition;
				Path outPath = new Path(offsetOut, fileName);
				String content = KafkaETLCommons.getOffset(nodeId, _topic,
						partition);
				KafkaETLUtils.writeText(fs, outPath, content);
				System.out.println("Dump " + content + " to "
						+ outPath.toUri().toString());
			}
		}
		in.close();

		return offsetOut;

	}

	protected Path createOutputDir(FileSystem fs, String root)
	throws Exception {
		return createOutputDir(fs, new Path(root), _timestamp);
	}

	protected Path createOutputDir(FileSystem fs, Path root) 
	throws Exception {
		return createOutputDir(fs, root, _timestamp);
	}

	protected Path createOutputDir(FileSystem fs, Path root, long timestamp)
	throws Exception {

		Path offsetOut = new Path(root, timeFormatter.print(timestamp));

		if (!fs.exists(offsetOut))
			fs.mkdirs(offsetOut);
		return offsetOut;
	}

	/**
	 * Helper function to initialize a job configuration
	 * @param id			job name
	 * @return
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	protected JobConf getJobConf(String id) throws Exception {
		JobConf conf = new JobConf();
		// set custom class loader with custom find resource strategy.

		conf.setJobName(id);
		String hadoop_ugi = _props.getProperty("hadoop.job.ugi", null);
		if (hadoop_ugi != null) {
			conf.set("hadoop.job.ugi", hadoop_ugi);
		}

		if (_props.getBoolean("is.local", false)) {
			conf.set("mapred.job.tracker", "local");
			conf.set("fs.default.name", "file:///");
			conf.set("mapred.local.dir", "/tmp/map-red");

			info("Running locally, no hadoop jar set.");
		} else {
			setClassLoaderAndJar(conf, getClass());
			info("Setting hadoop jar file for class:" + getClass() + "  to "
					+ conf.getJar());
			info("*************************************************************************");
			info("          Running on Real Hadoop Cluster("
					+ conf.get("mapred.job.tracker") + ")           ");
			info("*************************************************************************");
		}

		// set JVM options if present
		if (_props.containsKey("mapred.child.java.opts")) {
			conf.set("mapred.child.java.opts",
					_props.getProperty("mapred.child.java.opts"));
			info("mapred.child.java.opts set to "
					+ _props.getProperty("mapred.child.java.opts"));
		}

		// Adds External jars to hadoop classpath
		String externalJarList = _props.getProperty("hadoop.external.jarFiles", null);
		if (externalJarList != null) {
			String[] jarFiles = externalJarList.split(",");
			for (String jarFile : jarFiles) {
				info("Adding extenral jar File:" + jarFile);
				DistributedCache.addFileToClassPath(new Path(jarFile), conf);
			}
		}

		// Adds distributed cache files
		String cacheFileList = _props.getProperty("hadoop.cache.files", null);
		if (cacheFileList != null) {
			String[] cacheFiles = cacheFileList.split(",");
			for (String cacheFile : cacheFiles) {
				info("Adding Distributed Cache File:" + cacheFile);
				DistributedCache.addCacheFile(new URI(cacheFile), conf);
			}
		}

		// Adds distributed cache files
		String archiveFileList = _props
				.getProperty("hadoop.cache.archives", null);
		if (archiveFileList != null) {
			String[] archiveFiles = archiveFileList.split(",");
			for (String archiveFile : archiveFiles) {
				info("Adding Distributed Cache Archive File:" + archiveFile);
				DistributedCache.addCacheArchive(new URI(archiveFile), conf);
			}
		}

		String hadoopCacheJarDir = _props.getProperty(
				"hdfs.default.classpath.dir", null);
		if (hadoopCacheJarDir != null) {
			FileSystem fs = FileSystem.get(conf);
			if (fs != null) {
				FileStatus[] status = fs
						.listStatus(new Path(hadoopCacheJarDir));

				if (status != null) {
					for (int i = 0; i < status.length; ++i) {
						if (!status[i].isDir()) {
							Path path = new Path(hadoopCacheJarDir, status[i]
									.getPath().getName());
							info("Adding Jar to Distributed Cache Archive File:"
									+ path);

							DistributedCache.addFileToClassPath(path, conf);
						}
					}
				} else {
					info("hdfs.default.classpath.dir " + hadoopCacheJarDir
							+ " is empty.");
				}
			} else {
				info("hdfs.default.classpath.dir " + hadoopCacheJarDir
						+ " filesystem doesn't exist");
			}
		}

		// May want to add this to HadoopUtils, but will await refactoring
		for (String key : getProps().stringPropertyNames()) {
			String lowerCase = key.toLowerCase();
			if (lowerCase.startsWith(HADOOP_PREFIX)) {
				String newKey = key.substring(HADOOP_PREFIX.length());
				conf.set(newKey, _props.getProperty(key));
			}
		}

		KafkaETLUtils.setPropsInJob(conf, _props);
		
		return conf;
	}

	public void info(String message) {
		System.out.println(message);
	}

	public static void setClassLoaderAndJar(JobConf conf,
			@SuppressWarnings("rawtypes") Class jobClass) {
		conf.setClassLoader(Thread.currentThread().getContextClassLoader());
		String jar = KafkaETLUtils.findContainingJar(jobClass, Thread
				.currentThread().getContextClassLoader());
		if (jar != null) {
			conf.setJar(jar);
		}
	}

}
