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

import java.util.Map;
import java.util.Properties;

/**
 * CommandLineParameterizer handles the task of replacing parameter placeholders
 * with their provided values. This is used to change a command line template
 * into an executable statement.
 * <p/>
 * The format for parameters is {variableName}. These variables appear in a
 * template and are substituted on demand.
 * 
 */

public class CommandParameterizer {

    public static final String HOST_NAME_PARAM = "hostName";
    public static final String HOST_USER_ID_PARAM = "hostUserId";
    public static final String SSH_PRIVATE_KEY_PARAM = "sshPrivateKey";
    public static final String KAFKA_ROOT_DIRECTORY_PARAM = "kafkaRootDirectory";
    public static final String KAFKA_BROKER_CONFIG_PARAM = "kafkaBrokerConfig";
    public static final String KAFKA_LOG_DIRECTORY_PARAM = "kafkaLogDirectory";
    public static final String KAFKA_BROKER_ID_PARAM = "kafkaBrokerId";
    public static final String SOURCE_DIRECTORY_PARAM = "sourceDirectory";
    public static final String PARAMETERS_PARAM = "parameters";
    public static final String CLASS_NAME_PARAM = "className";
    public static final String ENABLE_ZK_PARAM = "enableZk";
    public static final String ZK_CONFIG_PARAM = "zkConfig";
    public static final String ZK_CONNECT_PARAM = "zkConnect";
    public static final String ZK_PORT_PARAM = "zkPort";
    public static final String ZK_NODE_ID_PARAM = "zkNodeId";
    public static final String ZK_DATA_DIR_PARAM = "zkDataDir";
    
    
    public static enum CommandType {
    	COMMAND_ID,
    	RAW_COMMAND
    };
    
    private final String rawCommand;

    public CommandParameterizer(String value, CommandType type) {
    	if ( type.equals(CommandType.COMMAND_ID) ) {
    		
    		Properties properties = new Properties();
    		try {
    			properties.load(getClass().getClassLoader().getResourceAsStream("commands.properties"));
    		} catch(Exception e) {
    			throw new IllegalStateException(e);
        	}
    		rawCommand = properties.getProperty(value);
    		
    	} else {
    		rawCommand = value;
    	}
    }

    public String parameterize(Map<String, String> parameters) {
        String command = rawCommand;

        for(Map.Entry<String, String> parameter: parameters.entrySet())
        	command = replace(command,
                    		 "{" + parameter.getKey() + "}",
                    		 parameter.getValue());

        return command;
    }

    /**
     * Adopted from org.apache.commons.lang.StringUtils
     */
    public String replace(String text, String searchString, String replacement) {
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= 16 ;
        StringBuffer buf = new StringBuffer(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }
}
