package io.kgraph.kgiraffe.schema.util;

import graphql.execution.ExecutionContext;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;

/**
 * Wrapper utility class to bridge between Graphql-java11 and Graphql-java13 Api changes
 */
public class DataFetchingEnvironmentBuilder {

    public static DataFetchingEnvironmentImpl.Builder newDataFetchingEnvironment(
        DataFetchingEnvironment env) {
        return DataFetchingEnvironmentImpl.newDataFetchingEnvironment(env);
    }

    public static DataFetchingEnvironmentImpl.Builder newDataFetchingEnvironment() {
        return DataFetchingEnvironmentImpl.newDataFetchingEnvironment();
    }

    public static DataFetchingEnvironmentImpl.Builder newDataFetchingEnvironment(
        ExecutionContext executionContext) {
        return DataFetchingEnvironmentImpl.newDataFetchingEnvironment(executionContext);
    }

}
