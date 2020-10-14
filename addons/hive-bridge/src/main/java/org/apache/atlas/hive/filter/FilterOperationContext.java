package org.apache.atlas.hive.filter;

public class FilterOperationContext {
    private FilterStrategy filterStrategy;

    public FilterOperationContext(FilterStrategy filterStrategy){
        this.filterStrategy = filterStrategy;
    }

    public boolean evaluateStrategy(String databaseName){
        return filterStrategy.evaluate(databaseName);
    }
}
