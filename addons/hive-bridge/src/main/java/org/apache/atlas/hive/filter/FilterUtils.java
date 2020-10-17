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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

/**
 * This class is for the utils used by filtering feature
 **/
public final class FilterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FilterUtils.class);

    public static Set<String> getValidEntitySetFromList(List<String> validEntitiesList) {
        return new HashSet<String>(validEntitiesList);
    }

    public static String constructValidPath(String basePath, String fileName) {
        String fullFilterFilePathString = FilenameUtils.concat(basePath, fileName);
        LOG.debug("Full path constructed {}", fullFilterFilePathString);
        return fullFilterFilePathString;
    }

    public static List<String> trimWhitespacesAndRemoveEmptyItemsInList(List<String> list) {
        // Remove any leading or following whitespace characters
        ListIterator<String> iterator = list.listIterator();

        while (iterator.hasNext()) {
            String s = iterator.next();
            if (s == null || s.isEmpty() || s.trim().length() < 1) {
                iterator.remove();
            } else {
                iterator.set(s.trim());
            }
        }

        return list;
    }

    public static Path getFullHdfsPath(String basePath, String fileName) {
        String fullPathString = FilterUtils.constructValidPath(basePath, fileName);
        return new Path(fullPathString);
    }
}
