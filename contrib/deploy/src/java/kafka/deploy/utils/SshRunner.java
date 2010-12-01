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

import kafka.deploy.utils.command.CommandOutputListener;
import kafka.deploy.utils.command.CommandParameterizer;
import kafka.deploy.utils.command.CommandRemoteOperation;
import kafka.deploy.utils.command.ExitCodeCallable;
import kafka.deploy.utils.command.StdOutCommandOutputListener;
import kafka.deploy.utils.command.UnixCommand;
import kafka.deploy.utils.command.CommandParameterizer.CommandType;

public class SshRunner extends CommandRemoteOperation implements RemoteOperation  {

    private final Collection<String> hostNames;

    private final File sshPrivateKey;

    private final String hostUserId, className, params, kafkaRootDirectory ;

    public SshRunner(List<String> hostNames, 
                     File sshPrivateKey,
                     String hostUserId,
                     String kafkaRootDirectory,
                     String className, 
                     String params) {
      this.hostNames = hostNames;
      this.sshPrivateKey = sshPrivateKey;
      this.hostUserId = hostUserId;
      this.kafkaRootDirectory = kafkaRootDirectory;
      this.className = className;
      this.params = params;
    }

    public void execute() throws RemoteOperationException {
        if(logger.isInfoEnabled())
            logger.info("Started remote class " + className);

        CommandParameterizer commandLineParameterizer = new CommandParameterizer("SshRunner.ssh"
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
            parameters.put(CommandParameterizer.CLASS_NAME_PARAM, className);
            
            CommandParameterizer parameterizer = new CommandParameterizer(params, CommandType.RAW_COMMAND);
            parameters.put(CommandParameterizer.PARAMETERS_PARAM, parameterizer.parameterize(parameters));

            hostNameCommandLineMap.put(hostName, commandLineParameterizer.parameterize(parameters));
        }

        execute(hostNameCommandLineMap);
    }

    @Override
    protected Callable<?> getCallable(UnixCommand command) {
        CommandOutputListener commandOutputListener = new StdOutCommandOutputListener(null, true);
        return new ExitCodeCallable(command, commandOutputListener);
    }

}
