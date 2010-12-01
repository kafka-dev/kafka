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

package kafka.deploy.utils;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.deploy.utils.command.CommandOutputListener;
import kafka.deploy.utils.command.CommandParameterizer;
import kafka.deploy.utils.command.CommandRemoteOperation;
import kafka.deploy.utils.command.LoggingCommandOutputListener;
import kafka.deploy.utils.command.UnixCommand;
import kafka.deploy.utils.command.CommandParameterizer.CommandType;

public class SshZkStarter extends CommandRemoteOperation implements RemoteOperation  {

	private final AtomicInteger completedCounter = new AtomicInteger();

    private final CommandOutputListener outputListener = new SshZkStarterCommandOutputListener();

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String kafkaRootDirectory, zkConfigFile, zkDataDir;
    
    private final Integer zkPort;

    public SshZkStarter(List<String> hostNames, 
                             File sshPrivateKey,
                             String hostUserId, 
                             String kafkaRootDirectory, 
                             String zkConfigFile, 
                             String zkDataDir,
                             Integer zkPort) {
      this.hostNames = hostNames;
      this.sshPrivateKey = sshPrivateKey;
      this.hostUserId = hostUserId;
      this.kafkaRootDirectory = kafkaRootDirectory;
      this.zkConfigFile = zkConfigFile;
      this.zkDataDir = zkDataDir;
      this.zkPort = zkPort;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Starting Zookeeper cluster");

        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();
        
        CommandParameterizer commandLineParameterizer = new CommandParameterizer("SshZkStarterMid", CommandType.COMMAND_ID), 
                             commandLineParameterizer2;
        
        // Generate server.<node_id>=<host_name>:2888:3888 list
        StringBuilder builder = new StringBuilder();
        if ( hostNames.size() > 1) {
        	Map<String, String> parameters = new HashMap<String, String>();
        	parameters.put(CommandParameterizer.ZK_CONFIG_PARAM, zkConfigFile);
        
        	int nodeId = 1;
        	for ( String hostName : hostNames) {
        		parameters.put(CommandParameterizer.ZK_NODE_ID_PARAM, Integer.toString(nodeId));
        		parameters.put(CommandParameterizer.HOST_NAME_PARAM, hostName);
        		builder.append(commandLineParameterizer.parameterize(parameters));
        		nodeId++;
        	}
        }
        
        // Generate commands for every host 
        int nodeId = 1;
        for(String hostName: hostNames) {
        	commandLineParameterizer = new CommandParameterizer("SshZkStarter.ssh" + (sshPrivateKey != null ? ""
                    																    : ".nokey"), 
                    																    CommandType.COMMAND_ID);
        	commandLineParameterizer2 = new CommandParameterizer("SshZkStarterEnd", CommandType.COMMAND_ID);
        	
        	Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(CommandParameterizer.HOST_NAME_PARAM, hostName);
            parameters.put(CommandParameterizer.HOST_USER_ID_PARAM, hostUserId);
            parameters.put(CommandParameterizer.SSH_PRIVATE_KEY_PARAM,
                           sshPrivateKey != null ? sshPrivateKey.getAbsolutePath() : null);
            parameters.put(CommandParameterizer.KAFKA_ROOT_DIRECTORY_PARAM, kafkaRootDirectory);
            parameters.put(CommandParameterizer.ZK_CONFIG_PARAM, zkConfigFile);
            parameters.put(CommandParameterizer.ZK_PORT_PARAM, Integer.toString(zkPort));
            parameters.put(CommandParameterizer.ZK_NODE_ID_PARAM, Integer.toString(nodeId));
            parameters.put(CommandParameterizer.ZK_DATA_DIR_PARAM, zkDataDir);

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters) 
            		                             + builder.toString() 
            		                             + commandLineParameterizer2.parameterize(parameters));
            nodeId++;
        }

        execute(hostNameCommandLineMap);
        
        if(logger.isInfoEnabled())
          logger.info("Startup of Zookeeper complete");
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger,
                                                                                       true);
        return new ZkStarterCallable<Object>(command, commandOutputListener);
    }

    public class SshZkStarterCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Snapshotting")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " startup complete");

                    if(hasStartupCompleted())
                        logger.info("Zookeeper cluster startup complete");
                }
            }
        }
    }
    
    private boolean hasStartupCompleted() {
        return hostNames.size() == completedCounter.get();
    }
    
    private class ZkStarterCallable<T> implements Callable<T> {

        private final UnixCommand command;

        private final CommandOutputListener commandOutputListener;

        public ZkStarterCallable(UnixCommand command,
                                      CommandOutputListener commandOutputListener) {
            this.command = command;
            this.commandOutputListener = commandOutputListener;
        }

        public T call() throws Exception {
            int exitCode = command.execute(commandOutputListener);

            // If the user hits Ctrl+C after startup, we get an exit code of
            // 255, so don't throw an exception in this case. Also, if we kill
            // the process via its PID, we can get an error code of 143 (see
            // http://forums.sun.com/thread.jspa?threadID=5136911).
            if(!(exitCode == 143 || exitCode == 255) && hasStartupCompleted())
                throw new Exception("Process on " + command.getHostName() + " exited with code "
                                    + exitCode + ". Please check the logs for details.");

            return null;
        }
    }

}
