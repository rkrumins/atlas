package org.apache.atlas.hive.filter;

import java.util.Set;

public class SourcesSetOperation implements FilterStrategy {

    Set<String> validDatabasesSet;

    public SourcesSetOperation(Set<String> validDatabasesSet) {
        this.validDatabasesSet = validDatabasesSet;
    }

    @Override
    public boolean evaluate(String databaseName) {
        return validDatabasesSet.contains(databaseName);
    }
}
