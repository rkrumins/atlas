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

import org.apache.atlas.hive.hook.HiveHook;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LocalFileClient implements FilterFileClient {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileClient.class);

    public String filterFileLocation;
    public String filterFileName;

    public LocalFileClient(String filterFileLocation, String filterFileName) {
        this.filterFileLocation = filterFileLocation;
        this.filterFileName = filterFileName;
    }

    public static List<String> loadSourcesConfigFile(String filePath) {
        LOG.info("HiveHook: Loading source config file");
        File configFile = new File(filePath);
        List<String> validEntityList = null;
        try {
            validEntityList = FileUtils.readLines(configFile);
            for (String validEntity : validEntityList) {
                validEntity.toLowerCase();
            }
            LOG.info("LocalFileClient: Loaded source config file successfully from path: " + filePath);
        } catch (IOException e) {
            LOG.error("LocalFileClient: Issue occurred when parsing source entity list");
            e.printStackTrace();
        }
        return validEntityList;
    }

    @Override
    public List<String> getValidSources() {
        String fullFilePath = FilterUtils.constructValidPath(filterFileLocation, filterFileName);
        return loadSourcesConfigFile(fullFilePath);
    }
}
