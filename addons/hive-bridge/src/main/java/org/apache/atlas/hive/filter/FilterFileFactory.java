/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.hive.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for filter file factory.
 * Filter file will be loaded from the source based on the configuration.
 **/
public class FilterFileFactory {
    private static final Logger LOG = LoggerFactory.getLogger(FilterFileFactory.class);

    public FilterFileClient getValidSources(String fileClientType, String filterFileLocation, String filterFileName){
        if (fileClientType == null) {
            LOG.error("No implementation for source system for filter file can be obtained as fileClientType is null");
            return null;
        }
        if (fileClientType.equalsIgnoreCase("hdfs")) {
            LOG.info("Filter file is being loaded from HDFS file system");
            return new HadoopFileClient(filterFileLocation, filterFileName);

        } else if (fileClientType.equalsIgnoreCase("local")) {
            LOG.info("Filter file is being loaded from local file system");
            return new LocalFileClient(filterFileLocation, filterFileName);
        } else {
            LOG.error("Could not initialize implementation for FilterFileClient as fileClientType does not match any of existing implementations");
        }

        return null;
    }
}
