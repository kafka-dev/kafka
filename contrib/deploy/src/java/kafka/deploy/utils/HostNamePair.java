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

/**
 * HostNamePair represents a pairing of a host's external and internal names.
 * For systems which have only one name, both the external and internal host
 * names should be the same. That is, they should be identical and neither
 * should be set to null.
 * 
 */
public class HostNamePair {

    private final String externalHostName;

    private final String internalHostName;

    public HostNamePair(String externalHostName, String internalHostName) {
        if(externalHostName == null)
            throw new IllegalArgumentException("externalHostName must be non-null");

        if(internalHostName == null)
            throw new IllegalArgumentException("externalHostName must be non-null");

        this.externalHostName = externalHostName;
        this.internalHostName = internalHostName;
    }

    public String getExternalHostName() {
        return externalHostName;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + externalHostName.hashCode();
        result = prime * result + internalHostName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if(this == other)
            return true;

        if(!(other instanceof HostNamePair))
            return false;

        HostNamePair hnp = (HostNamePair) other;

        return hnp.externalHostName.equals(externalHostName)
               && hnp.internalHostName.equals(internalHostName);
    }

}
