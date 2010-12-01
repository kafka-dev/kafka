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
import kafka.deploy.utils.RsyncDeployer;

public class KafkaDeployerApp extends KafkaApp {

    public static void main(String[] args) throws Exception {
        new KafkaDeployerApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "kafka-deployer.sh";
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
        parser.accepts("kafkaroot", "Root directory on remote host (default:~/kafka)")
              .withRequiredArg();
        parser.accepts("source", "The source directory on the local machine")
              .withRequiredArg();
        parser.accepts("useinternal", "Use internal host name (default:false)")
        	  .withOptionalArg();

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File sshPrivateKey = getInputFile(options, "sshprivatekey");
        String hostUserId = KafkaApp.valueOf(options, "hostuserid", "root");
        File sourceDirectory = getRequiredInputFile(options, "source");
        String kafkaRootDirectory = KafkaApp.valueOf(options, "kafkaroot", "kafka");
        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(hostNamesFile);
        List<String> hostNames = new ArrayList<String>();
        boolean useInternal = options.has("useinternal");

        for(HostNamePair hostNamePair: hostNamePairs) {
        	if ( useInternal ) 
        		hostNames.add(hostNamePair.getInternalHostName());
        	else
        		hostNames.add(hostNamePair.getExternalHostName());
        }

        RemoteOperation operation = new RsyncDeployer(hostNames,
                                                      sshPrivateKey,
                                                      hostUserId,
                                                      sourceDirectory,
                                                      kafkaRootDirectory);
        operation.execute();
    }

}
