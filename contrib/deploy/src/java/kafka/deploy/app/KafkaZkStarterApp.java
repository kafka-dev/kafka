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
import java.util.List;

import joptsimple.OptionSet;
import kafka.deploy.utils.HostNamePair;
import kafka.deploy.utils.RemoteOperation;
import kafka.deploy.utils.RemoteOperationException;
import kafka.deploy.utils.SshZkStarter;
import kafka.deploy.utils.SshZkStopper;

public class KafkaZkStarterApp extends KafkaApp {

    public static void main(String[] args) throws Exception {
        new KafkaZkStarterApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "kafka-zk-start.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("zkhosts", "File containing host names on which to run ZK")
              .withRequiredArg();
        parser.accepts("sshprivatekey", "File containing SSH private key (optional)")
              .withRequiredArg();
        parser.accepts("hostuserid", "User ID on remote host (default:root)")
              .withRequiredArg();
        parser.accepts("kafkaroot", "Kafka's root directory on remote host")
              .withRequiredArg();
        parser.accepts("zkconfig", "Kafka's config file on remote host")
              .withRequiredArg();
        parser.accepts("useinternal", "Use internal host name (default:false)")
              .withOptionalArg();
        parser.accepts("zkport", "Zookeeper port number (default:2181 ) ")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("zkdatadir", "Zookeeper data directory (default:/tmp/zookeeper)")
        	  .withRequiredArg();
        	  

        OptionSet options = parse(args);
        File zookeeperHostNamesFile = getRequiredInputFile(options, "zkhosts");
        
        // Parse host names
        List<HostNamePair> zookeeperHostNamePairs = getHostNamesPairsFromFile(zookeeperHostNamesFile);
        final List<String> zookeeperHostNames = new ArrayList<String>();
        final boolean useInternal = options.has("useinternal");
        final File sshPrivateKey = getInputFile(options, "sshprivatekey");
        final String hostUserId = KafkaApp.valueOf(options, "hostuserid", "root");
        final String kafkaRootDirectory = getRequiredString(options, "kafkaroot");
        final String zkConfigFile = getRequiredString(options, "zkconfig");
        Integer zkPort = KafkaApp.valueOf(options, "zkport", 2181);
        final String zkDataDir = KafkaApp.valueOf(options, "zkdatadir", "/tmp/zookeeper");
        
        for ( HostNamePair zookeeperHostName : zookeeperHostNamePairs) {
        	if ( useInternal ) {
        		zookeeperHostNames.add(zookeeperHostName.getInternalHostName());
        	} else {
        		zookeeperHostNames.add(zookeeperHostName.getExternalHostName());
        	}
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                RemoteOperation operation = new SshZkStopper(zookeeperHostNames,
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

        RemoteOperation operation = new SshZkStarter(zookeeperHostNames,
                                                     sshPrivateKey,
                                                     hostUserId,
                                                     kafkaRootDirectory,
                                                     zkConfigFile,
                                                     zkDataDir,
                                                     zkPort);

        operation.execute();
    }
}
