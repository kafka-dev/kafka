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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import kafka.deploy.utils.RemoteOperationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * CommandRemoteOperation represents a command-line based remote operation.
 * That is, it invokes the remote system by means of invoking a command line
 * executable.
 * 
 * <p/>
 * 
 * What CommandRemoteOperation solves for its subclasses is converting
 * per-host String commands to invokable UnixCommand objects that are then
 * executed in parallel against the remote hosts. The results of those command
 * line invocations are then placed (in arbitrary order) into a list that is
 * returned to the caller.
 * 
 * 
 * @param <T> Return type for the operation, specific to the subclass
 *        implementation
 */
public abstract class CommandRemoteOperation {

    protected final Log logger = LogFactory.getLog(getClass());

    /**
     * Executes the given commands in parallel on the remote hosts and
     * aggregates the results for the caller.
     * 
     * @param hostNameCommandLineMap Map with a key is the external host name
     *        and the value is the command line to execute remotely on that host
     * 
     * @return List of result types as dictated by the subclass
     * 
     * @throws RemoteOperationException Thrown on error invoking the command on
     *         one or more clients.
     */
    protected void execute(Map<String, String> hostNameCommandLineMap)
            throws RemoteOperationException {

        ExecutorService threadPool = Executors.newFixedThreadPool(hostNameCommandLineMap.size());
        List<Future<?>> futures = new ArrayList<Future<?>>();

        for(Map.Entry<String, String> entry: hostNameCommandLineMap.entrySet()) {
            String hostName = entry.getKey();
            String commandLine = entry.getValue();

            if(logger.isDebugEnabled())
                logger.debug("Command to execute: " + commandLine);

            List<String> commandArgs = parse(commandLine);
            UnixCommand command = new UnixCommand(hostName, commandArgs);
            Callable<?> callable = getCallable(command);
            Future<?> future = threadPool.submit(callable);
            futures.add(future);
        }

        // Build up a list of all the results and/or capture the errors as they
        // occur.
        try {
            StringBuilder errors = new StringBuilder();

            for(Future<?> future: futures) {
                Throwable t = null;

                try {
                    future.get();
                } catch(ExecutionException ex) {
                    t = ex.getCause();
                } catch(Exception e) {
                    t = e;
                }

                if(t != null) {
                    if(logger.isWarnEnabled())
                        logger.warn(t, t);

                    if(errors.length() > 0)
                        errors.append("; ");

                    errors.append(t.getMessage());
                }
            }

            if(errors.length() > 0)
                throw new RemoteOperationException(errors.toString());
        } finally {
            threadPool.shutdown();

            try {
                threadPool.awaitTermination(60, TimeUnit.SECONDS);
            } catch(InterruptedException e) {
                if(logger.isWarnEnabled())
                    logger.warn(e, e);
            }
        }
    }

    /**
     * By default we'll add a logging listener to output the output from the
     * remote invocation and return the result of the ExitCodeCallable.
     * 
     * <p/>
     * 
     * This method can be overridden by subclasses to provide a more useful
     * interaction with the output from the remote host.
     * 
     * @param command UnixCommand to execute
     * 
     * @return Callable that is used in the thread pool in the execute method
     */
    protected Callable<?> getCallable(UnixCommand command) {
      CommandOutputListener commandOutputListener = new LoggingCommandOutputListener(null,
                                                                                     logger,
                                                                                     true);
      return new ExitCodeCallable(command, commandOutputListener);
    }
    
    public List<String> parse(String command) {
      List<String> commands = new ArrayList<String>();
      boolean isInQuotes = false;
      int start = 0;

      for(int i = 0; i < command.length(); i++) {
          char c = command.charAt(i);

          if(c == '\"') {
              isInQuotes = !isInQuotes;
          } else if(c == ' ' && !isInQuotes) {
              String substring = command.substring(start, i).trim();
              start = i + 1;

              if(substring.trim().length() > 0)
                  commands.add(substring.replace("\"", ""));
          }
      }

      String substring = command.substring(start).trim();

      if(substring.length() > 0)
          commands.add(substring.replace("\"", ""));

      return commands;
  }

}
