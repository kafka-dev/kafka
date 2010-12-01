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

package kafka.deploy.utils.command;

/**
 * One of the main ways we determine what's going on is to literally parse the
 * output from the remote command line invocation. Each remote operation is
 * different and needs to be implemented specifically for the target command.
 * 
 */

public interface CommandOutputListener {

    /**
     * Called by the UnixCommand as it receives a line of output and calls any
     * listener that was provided.
     * 
     * @param hostName Host name from which the line of output
     *        originated
     * @param line Line of output from remote system
     */
    public void outputReceived(String hostName, String line);

}
