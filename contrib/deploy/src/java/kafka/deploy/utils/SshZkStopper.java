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
import java.util.Map;
import java.util.concurrent.Callable;

import kafka.deploy.utils.command.CommandOutputListener;
import kafka.deploy.utils.command.CommandParameterizer;
import kafka.deploy.utils.command.CommandRemoteOperation;
import kafka.deploy.utils.command.LoggingCommandOutputListener;
import kafka.deploy.utils.command.UnixCommand;
import kafka.deploy.utils.command.CommandParameterizer.CommandType;

public class SshZkStopper extends CommandRemoteOperation implements RemoteOperation  {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final String kafkaRootDirectory;

    private final boolean suppressErrors;

    public SshZkStopper(Collection<String> hostNames,
                        File sshPrivateKey,
                        String hostUserId,
                        String kafkaRootDirectory,
                        boolean suppressErrors) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.kafkaRootDirectory = kafkaRootDirectory;
        this.suppressErrors = suppressErrors;
    }

    @Override
    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Stopping Zookeeper cluster");

        CommandParameterizer commandLineParameterizer = new CommandParameterizer("SshZkStopper.ssh"
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

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Stopping of Zookeeper cluster complete");
    }

    @Override
    protected Callable<?> getCallable(final UnixCommand command) {
        if(suppressErrors) {
            final CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null,
                                                                                                 logger,
                                                                                                 true);
            return new Callable<Object>() {

                public Object call() throws Exception {
                    try {
                        command.execute(commandOutputListener);
                    } catch(Exception e) {
                        // Ignore as we're suppressing errors...
                    }

                    return null;
                }

            };
        } else {
            return super.getCallable(command);
        }
    }
}
