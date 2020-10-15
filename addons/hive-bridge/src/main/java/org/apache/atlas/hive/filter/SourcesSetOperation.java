package org.apache.atlas.hive.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class SourcesSetOperation implements FilterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(SourcesSetOperation.class);

    Set<String> validDatabasesSet;

    public SourcesSetOperation(Set<String> validDatabasesSet) {
        this.validDatabasesSet = validDatabasesSet;
    }

    @Override
    public boolean evaluate(String databaseName) {
        boolean validFlag = validDatabasesSet.contains(databaseName);
        LOG.debug("Flag value is {} for database {} in pre-defined valid databases set", databaseName, validFlag);
        return validFlag;
    }
}
