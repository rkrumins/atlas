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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * This is Hadoop File System Client for reading source entities list from Hadoop Distributed File System (HDFS)
 */
public class HadoopFileClient implements FilterFileClient {
    public String filterFileLocation;
    public String filterFileName;

    private static final Logger LOG = LoggerFactory.getLogger(HadoopFileClient.class);

    private static final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");

    public HadoopFileClient(String filterFileLocation, String filterFileName) {
        this.filterFileLocation = filterFileLocation;
        this.filterFileName = filterFileName;
    }

//    private Configuration loadHadoopConfiguration() {
//        Configuration conf = new Configuration();
//        conf.addResource(new Path("file://" + HADOOP_CONF_DIR + "/core-site.xml"));
//        conf.addResource(new Path("file://" + HADOOP_CONF_DIR + "/hdfs-site.xml"));
//        return conf;
//    }

    @Override
    public List<String> getValidSources() {
        LOG.debug("HADOOP_CONF_DIR value is set to " + HADOOP_CONF_DIR);

        // Initialise Hadoop Configuration
        Configuration conf = new Configuration();

        LOG.debug("Printing configuration for Hadoop");
        LOG.debug(conf.toString());

        List<String> validEntitiesListFromFile = null;
        try {
            // Get the HDFS filesystem
            FileSystem fs = FileSystem.get(conf);

            LOG.debug("Reading file from hdfs");

            String fullPathString = FilterUtils.constructValidPath(filterFileLocation, filterFileName);
            Path hdfsPath = new Path(fullPathString);
            LOG.info("Full path on HDFS set to for Atlas Hive Hook filter file {}", hdfsPath.toString());

            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsPath);

            validEntitiesListFromFile = IOUtils.readLines(inputStream, StandardCharsets.UTF_8);
            LOG.debug("Loaded filter file for Atlas Hive Hook from HDFS with {} items in valid entities list", validEntitiesListFromFile.size());

            inputStream.close();
            fs.close();
        } catch (IOException e) {
            LOG.error("Failed to load file from HDFS at this path {}/{}", filterFileLocation, filterFileName);
            e.printStackTrace();
        }

        if (validEntitiesListFromFile != null) {
            LOG.debug("Valid entities list size " + validEntitiesListFromFile.size());
            LOG.debug("Valid entities list contents below: ");
            LOG.debug(Arrays.toString(validEntitiesListFromFile.toArray()));
        } else {
            LOG.error("List is empty for valid entities");
        }

        return validEntitiesListFromFile;
    }
}
