package org.apache.atlas.hive.filter;

public interface FilterStrategy {
    boolean evaluate(String databaseName);
}
