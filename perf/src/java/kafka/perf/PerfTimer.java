package kafka.perf;

import java.io.BufferedWriter;
import java.io.FileWriter;

import kafka.perf.jmx.BrokerJmxClient;

public class PerfTimer extends Thread
{
  private final long timeToRun;
  private final BrokerJmxClient brokerStats;
  private final KafkaPerfSimulator perfSim;
  private final int numConsumers, numProducer,numParts, numTopic;
  private final String reportFile;
  public PerfTimer(BrokerJmxClient brokerStats,
                   KafkaPerfSimulator perfSim, int numConsumers, 
                   int numProducer, int numParts, int numTopic,
                   long timeToRun,
                   String fileName)
  {
    this.timeToRun = timeToRun;
    this.brokerStats = brokerStats;
    this.perfSim = perfSim;
    this.numConsumers = numConsumers;
    this.numProducer = numProducer;
    this.numParts = numParts;
    this.numTopic = numTopic;
    reportFile = fileName;
  }

  public void printReport() throws Exception
  {
    FileWriter fstream = new FileWriter(reportFile, true);
    BufferedWriter writer = new BufferedWriter(fstream);
    String header = "#consumers, #of producers, #of partitions, #of topic, " +
          "consumer mess/sec,consumer MB/sec, producer mess/sec,producer MB/sec, broker MB write/sec, broker MB read/sec";
    String data = numConsumers+ "," + numProducer + "," + numParts+ "," + numTopic + "," +
                       perfSim.getAvgMessagesRecPs() + "," +
                       perfSim.getAvgMBytesRecPs() + "," +
                       perfSim.getAvgMessagesSentPs() + "," +
                       perfSim.getAvgMBytesSentPs() + "," +
                       brokerStats.getBrokerStats();
    
    System.out.println(header);
    System.out.println(data);
    writer.write(data);
    writer.newLine();
    writer.flush();
    writer.close();
    fstream.close();
    
  }

  public void run() {
    try
    {
      Thread.sleep(timeToRun);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
    
    try
    {
      printReport();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    System.exit(0);
  }
}