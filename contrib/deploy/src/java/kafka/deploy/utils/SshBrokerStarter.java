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

public class SshBrokerStarter extends CommandRemoteOperation implements RemoteOperation  {

    private final AtomicInteger completedCounter = new AtomicInteger();

    private final CommandOutputListener outputListener = new SshBrokerStarterCommandOutputListener();

    private final Collection<String> hostNames, zkHostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String kafkaRootDirectory, kafkaConfig, kafkaLog;

    private final Map<String, Integer> brokerIds;
    
    private final Integer zkPort;

    /**
     * Creates a new SshBrokerStarter instance.
     * 
     * @param hostNames External host names for the brokers
     * @param sshPrivateKey SSH private key file on local filesystem that can
     *        access all of the remote hosts, or null if not needed
     * @param hostUserId User ID on the remote hosts; assumed to be the same for
     *        all of the remote hosts
     * @param brokerIds Mapping of host names to broker ids
     * @param kafkaRootDirectory Directory pointing to the Kafka
     *        distribution, relative to the home directory of the user on the
     *        remote system represented by hostUserId; assumed to be the same
     *        for all of the remote hosts
     * @param kafkaConfig Location of the config file on the remote host
     * @param kafkaLog Location of the Kafka logs
     * @param zkHostNames Names of all the zookeeper cluster nodes
     * @param zkPort The port on which zookeeper listens
     */
    public SshBrokerStarter(List<String> hostNames, 
                             File sshPrivateKey,
                             String hostUserId, 
                             Map<String, Integer> brokerIds,
                             String kafkaRootDirectory, 
                             String kafkaConfig, 
                             String kafkaLog, 
                             List<String> zkHostNames,
                             Integer zkPort) {
      this.hostNames = hostNames;
      this.sshPrivateKey = sshPrivateKey;
      this.hostUserId = hostUserId;
      this.brokerIds = brokerIds;
      this.kafkaRootDirectory = kafkaRootDirectory;
      this.kafkaConfig = kafkaConfig;
      this.kafkaLog = kafkaLog;
      this.zkHostNames = zkHostNames;
      this.zkPort = zkPort;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Starting Kafka brokers");

        boolean enableZk = (zkHostNames != null && zkPort != null);
        
        // Generate zk.connect string i.e. <host>:<port>,<host>:<port>
        StringBuilder builder = new StringBuilder();
        if ( enableZk ) {
        	for ( String zkHostName : zkHostNames) {
        		builder.append( zkHostName + ":" + zkPort + ",");
        	}
        }
        
        CommandParameterizer commandLineParameterizer = new CommandParameterizer("SshBrokerStarter.ssh"
                                                                                 + (sshPrivateKey != null ? ""
                                                                                 : ".nokey"), 
                                                                                 CommandType.COMMAND_ID);
        
        Map<String, String> hostNameCommandLineMap = new HashMap<String, String>();

        for(String hostName: hostNames) {
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(CommandParameterizer.HOST_NAME_PARAM, hostName);
            parameters.put(CommandParameterizer.HOST_USER_ID_PARAM, hostUserId);
            parameters.put(CommandParameterizer.SSH_PRIVATE_KEY_PARAM,
                           sshPrivateKey != null ? sshPrivateKey.getAbsolutePath() : null);
            parameters.put(CommandParameterizer.KAFKA_ROOT_DIRECTORY_PARAM, kafkaRootDirectory);
            parameters.put(CommandParameterizer.KAFKA_BROKER_CONFIG_PARAM, kafkaConfig);
            parameters.put(CommandParameterizer.KAFKA_LOG_DIRECTORY_PARAM, kafkaLog);
            parameters.put(CommandParameterizer.KAFKA_BROKER_ID_PARAM, brokerIds.get(hostName).toString());
            parameters.put(CommandParameterizer.ENABLE_ZK_PARAM, enableZk ? "true" : "false");
            parameters.put(CommandParameterizer.ZK_CONNECT_PARAM, enableZk ? builder.toString() : "");
            
            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        execute(hostNameCommandLineMap);
        
        if(logger.isInfoEnabled())
          logger.info("Startup complete");
    }

    @Override
    protected Callable<Object> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(outputListener,
                                                                                       logger,
                                                                                       true);
        return new BrokerStarterCallable<Object>(command, commandOutputListener);
    }

    public class SshBrokerStarterCommandOutputListener implements CommandOutputListener {

        public void outputReceived(String hostName, String line) {
            if(line.contains("Server started")) {
                completedCounter.incrementAndGet();

                if(logger.isInfoEnabled()) {
                    logger.info(hostName + " startup complete");

                    if(hasStartupCompleted())
                        logger.info("Broker startup complete");
                }
            }
        }
    }

    private boolean hasStartupCompleted() {
        return hostNames.size() == completedCounter.get();
    }

    private class BrokerStarterCallable<T> implements Callable<T> {

        private final UnixCommand command;

        private final CommandOutputListener commandOutputListener;

        public BrokerStarterCallable(UnixCommand command,
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
            if(!((exitCode == 143 || exitCode == 255) && hasStartupCompleted()))
                throw new Exception("Process on " + command.getHostName() + " exited with code "
                                    + exitCode + ". Please check the logs for details.");

            return null;
        }
    }

}
