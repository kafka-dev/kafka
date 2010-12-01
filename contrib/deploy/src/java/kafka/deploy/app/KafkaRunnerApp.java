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
import kafka.deploy.utils.SshRunner;

public class KafkaRunnerApp extends KafkaApp {

    public static void main(String[] args) throws Exception {
        new KafkaRunnerApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "kafka-runner.sh";
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
        parser.accepts("hostuserid", "User ID on remote host")
              .withRequiredArg();
        parser.accepts("useinternal", "Use internal host name (default:false)")
        	  .withOptionalArg();
        parser.accepts("kafkaroot", "Kafka's root directory on remote host")
        	  .withRequiredArg();
        parser.accepts("classname", "Full classname to run remotely")
              .withRequiredArg();
        parser.accepts("parameters", "Parameters to run remotely for the classname")
        	  .withRequiredArg()
        	  .describedAs("key=value");

        OptionSet options = parse(args);
        File hostNamesFile = getRequiredInputFile(options, "hostnames");
        File sshPrivateKey = getInputFile(options, "sshprivatekey");
        String hostUserId = KafkaApp.valueOf(options, "hostuserid", "root");
        String className = getRequiredString(options, "classname");
        List<String> parametersList = getRequiredListString(options, "parameters");
        
        StringBuilder builder = new StringBuilder();
        for ( String param: parametersList) {
        	String[] keyValue = param.split("=", 2);
        	if ( keyValue.length == 1) {
        		builder.append("--" + keyValue[0] + " ");
        	} else {
        		builder.append("--" + keyValue[0] + " " + keyValue[1] + " ");
        	}
        }
        String kafkaRootDirectory = getRequiredString(options, "kafkaroot");

        List<HostNamePair> hostNamePairs = getHostNamesPairsFromFile(hostNamesFile);
        List<String> hostNames = new ArrayList<String>();
        boolean useInternal = options.has("useinternal");

        for(HostNamePair hostNamePair: hostNamePairs) {
        	if ( useInternal ) 
        		hostNames.add(hostNamePair.getInternalHostName());
        	else
        		hostNames.add(hostNamePair.getExternalHostName());
        }
        
        RemoteOperation operation = new SshRunner(hostNames,
                                                  sshPrivateKey,
                                                  hostUserId,
                                                  kafkaRootDirectory, 
                                                  className, 
                                                  builder.toString());
        operation.execute();
    }

}
