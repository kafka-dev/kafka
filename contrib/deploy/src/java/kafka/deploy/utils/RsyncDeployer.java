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

import kafka.deploy.utils.command.CommandParameterizer;
import kafka.deploy.utils.command.CommandOutputListener;
import kafka.deploy.utils.command.CommandRemoteOperation;
import kafka.deploy.utils.command.ExitCodeCallable;
import kafka.deploy.utils.command.LoggingCommandOutputListener;
import kafka.deploy.utils.command.UnixCommand;
import kafka.deploy.utils.command.CommandParameterizer.CommandType;

/**
 * RsyncDeployer is an implementation of Deployer that uses the rsync command
 * line utility to copy the data to the remote host. 
 * 
 */
public class RsyncDeployer extends CommandRemoteOperation implements RemoteOperation {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId;

    private final File sourceDirectory;

    private final String kafkaRootDirectory;

    /**
     * Creates a new RsyncDeployer instance.
     * 
     * @param hostNames External host names to which we want to deploy Kafka
     * @param sshPrivateKey SSH private key file on local filesystem that can
     *        access all of the remote hosts, or null if not needed
     * @param sourceDirectory Directory of Kafka distribution on local file
     *        system
     * @param hostUserId User ID on the remote hosts; assumed to be the same for
     *        all of the remote hosts
     * @param kafkaRoot Parent directory into which the
     *        sourceDirectory will be copied, relative to the home directory of
     *        the user on the remote system represented by hostUserId
     */
    public RsyncDeployer(Collection<String> hostNames,
                         File sshPrivateKey,
                         String hostUserId,
                         File sourceDirectory,
                         String kafkaRootDirectory) {
        this.hostNames = hostNames;
        this.sshPrivateKey = sshPrivateKey;
        this.hostUserId = hostUserId;
        this.sourceDirectory = sourceDirectory;
        this.kafkaRootDirectory = kafkaRootDirectory;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Rsync-ing " + sourceDirectory.getAbsolutePath() + " to "
                        + kafkaRootDirectory + " on remote hosts: " + hostNames);

        if(!sourceDirectory.exists())
            throw new RemoteOperationException(sourceDirectory.getAbsolutePath()
                                               + " does not exist");

        if(!sourceDirectory.isDirectory())
            throw new RemoteOperationException("Directory " + sourceDirectory.getAbsolutePath()
                                               + " is not a directory");

        CommandParameterizer commandLineParameterizer = new CommandParameterizer("RsyncDeployer.rsync"
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
            parameters.put(CommandParameterizer.SOURCE_DIRECTORY_PARAM, sourceDirectory.getAbsolutePath());

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        execute(hostNameCommandLineMap);

        if(logger.isInfoEnabled())
            logger.info("Rsync-ing complete");
    }

    @Override
    protected Callable<?> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null,
                                                                                       logger,
                                                                                       false);
        return new ExitCodeCallable(command, commandOutputListener);
    }

}
