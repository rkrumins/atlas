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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is for the utils used by filtering feature
 **/
public final class FilterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FilterUtils.class);

    public static List<String> loadSourcesConfigFile(String filePath) {
        LOG.info("HiveHook: Loading source config file");
        File configFile = new File(filePath);
        List<String> validEntityList = null;
        try {
            validEntityList = FileUtils.readLines(configFile);
            for (String validEntity : validEntityList) {
                validEntity.toLowerCase();
            }
            LOG.info("HiveHook: Loaded source config file successfully from path: " + filePath);
        } catch (IOException e) {
            LOG.error("HiveHook: Issue occurred when parsing source entity list");
            e.printStackTrace();
        }
        return validEntityList;
    }

    public static Set<String> getValidEntitySetFromList(List<String> validEntitiesList) {
        return new HashSet<String>(validEntitiesList);
    }

//    public static boolean getValidMatchForRegexPattern(String databaseName, String regexPattern) {
//        Pattern pattern = Pattern.compile(regexPattern, Pattern.MULTILINE);
//        Matcher matcher = pattern.matcher(databaseName);
//        return matcher.matches();
//    }
//
//    public static boolean checkIfDatabaseIsOfValidRegexPattern(String databaseName, List<String> regexPatterns) {
//        boolean validPatternFlag = false;
//
//        for (String regexPattern: regexPatterns) {
//            if (getValidMatchForRegexPattern(databaseName, regexPattern)) {
//                validPatternFlag = true;
//                break;
//            }
//        }
//        return validPatternFlag;
//    }

    public static String constructValidPath(String filterFileLocation, String filterFileName) {
        String fullFilterFilePathString = FilenameUtils.concat(filterFileLocation, filterFileName);
        LOG.debug("Full path constructed {}", fullFilterFilePathString);
        return fullFilterFilePathString;
    }

    public static Set<String> getValidEntitySetFromList(String filterFileLocation, String filterFileName) {
        LOG.info("HiveHook: Source entity file location: " + filterFileLocation);
        LOG.info("HiveHook: Source entity filename: " + filterFileName);
        String sourceConfigFilePath = filterFileLocation + File.separator + filterFileName;
        Set<String> validEntitiesSet = new HashSet<String>(loadSourcesConfigFile(sourceConfigFilePath));
        LOG.info("Valid entities set to: " + Arrays.toString(validEntitiesSet.toArray()));
        return validEntitiesSet;
    }

    // This method checks if database for which event was triggered is a part of databases defined in source database list
    public static boolean getValidEntityFlag(String databaseName) {
        String filterFileFolderLocation = "";
        String filterFileName = "";
        Set<String> validList = getValidEntitySetFromList(filterFileFolderLocation, filterFileName);
        boolean validFlag = validList.contains(databaseName);
//        System.out.println("DEBUG: HiveHook for database " + databaseName + " valid entity flag is set to " + validFlag);
        return validFlag;
    }

    private static int getSourceListFromDatabase(Hive hiveClient, String databaseName, String tableName) throws HiveException {

        if (!StringUtils.isEmpty(databaseName)) {
            LOG.info("Getting table records for database ", databaseName);
            Table table = hiveClient.getTable(databaseName, tableName);
            LOG.info("Total number of records in table {} is {}", tableName);

        }
        return 0;
    }

}
