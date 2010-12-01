/*
 * Copyright 2010 LinkedIn, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package kafka.deploy.app;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joptsimple.OptionSet;
import kafka.deploy.utils.HostNamePair;
import kafka.deploy.utils.RemoteOperation;
import kafka.deploy.utils.RemoteOperationException;
import kafka.deploy.utils.SshBrokerStarter;
import kafka.deploy.utils.SshBrokerStopper;

public class KafkaBrokerStarterApp extends KafkaApp {

    public static void main(String[] args) throws Exception {
        new KafkaBrokerStarterApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "kafka-broker-start.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("hostnames", "File containing host names")
              .withRequiredArg();
        parser.accepts("sshprivatekey", "File containing SSH private key (optional)")
              .withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host (default:root)")
              .withRequiredArg();
        parser.accepts("kafkaroot", "Kafka's root directory on remote host")
              .withRequiredArg();
        parser.accepts("kafkaconfig", "Kafka's config file on remote host")
              .withRequiredArg();
        parser.accepts("useinternal", "Use internal host name (default:false)")
              .withOptionalArg();
        parser.accepts("kafkalog", "Kafka's log directory on remote host (default:/tmp/kafka-logs)")
              .withRequiredArg();
        parser.accepts("brokerids", "File containing mapping of broker hostnames to ids")
              .withRequiredArg();
        parser.accepts("zkhosts", "Zookeeper host file names")
        	  .withRequiredArg();
        parser.accepts("zkport", "Zookeeper port number ( Default:2181 ) ")
              .withRequiredArg()
              .ofType(Integer.class);

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File zkHostNamesFile = getOptionalInputFile(options, "zkhosts");
        List<String> zkHostNames = null;
        final boolean useInternal = options.has("useinternal");
        
        Integer zkPort = null;
        if ( zkHostNamesFile == null ) {
        	zkHostNames = null;
        } else {
        	 List<HostNamePair> zookeeperHostNamePairs = getHostNamesPairsFromFile(zkHostNamesFile);
        	 zkHostNames = new ArrayList<String>();
        	 for ( HostNamePair zookeeperHostName : zookeeperHostNamePairs) {
             	if ( useInternal ) {
             		zkHostNames.add(zookeeperHostName.getInternalHostName());
             	} else {
             		zkHostNames.add(zookeeperHostName.getExternalHostName());
             	}
             }
            zkPort =  KafkaApp.valueOf(options, "zkport", 2181);
        }
        
        // Parse host names
        final List<String> hostNames = new ArrayList<String>();
        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(hostNamesFile);
        for(HostNamePair hostNamePair: hostNamePairs) {
        	if ( useInternal ) {
        		hostNames.add(hostNamePair.getInternalHostName());
        	} else {
        		hostNames.add(hostNamePair.getExternalHostName());
        	}
        }
        
        final File sshPrivateKey = getInputFile(options, "sshprivatekey");
        final String hostUserId = KafkaApp.valueOf(options, "hostuserid", "root");
        final String kafkaRootDirectory = getRequiredString(options, "kafkaroot");
        String kafkaConfig = getRequiredString(options, "kafkaconfig");
        String kafkaLog = KafkaApp.valueOf(options, "kafkalog", "/tmp/kafka-logs");

        Map<String, Integer> brokerIds = null;
        if ( options.has("brokerids") ) {
            brokerIds = getBrokerIds(hostNames,
                                     getRequiredInputFile(options, "brokerids"));
        } else {
            brokerIds = new HashMap<String, Integer>();
            int brokerId = 0;
            for ( String hostName : hostNames) {
              brokerIds.put(hostName, brokerId++);
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                RemoteOperation operation = new SshBrokerStopper(hostNames,
                                                                 sshPrivateKey,
                                                                 hostUserId,
                                                                 kafkaRootDirectory, 
                                                                 true);
                try {
                    operation.execute();
                } catch(RemoteOperationException e) {
                    e.printStackTrace();
                }
            }

        });

        RemoteOperation operation = new SshBrokerStarter(hostNames,
                                                         sshPrivateKey,
                                                         hostUserId,
                                                         brokerIds,
                                                         kafkaRootDirectory,
                                                         kafkaConfig,
                                                         kafkaLog, 
                                                         zkHostNames, 
                                                         zkPort);

        operation.execute();
    }

    private Map<String, Integer> getBrokerIds(List<String> hostNames,
                                              File brokerIdFile) throws Exception {
        Map<String, Integer> brokerIds = new HashMap<String, Integer>();
        Map<String, String> props = getRequiredPropertiesFile(brokerIdFile);
        
        for ( String hostName : hostNames) {
          String brokerId = props.get(hostName);
          if ( brokerId != null ) {
            brokerIds.put(hostName, Integer.parseInt(brokerId));
          }
        }
        
        if(brokerIds.size() != hostNames.size()) {
            throw new Exception("The broker file " + 
                                brokerIdFile.getAbsolutePath() + 
                                " does not have correct number of brokers");
        }

        return brokerIds;
    }
}
