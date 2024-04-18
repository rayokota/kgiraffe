package io.kgraph.kgiraffe.schema.util;

import graphql.ExceptionWhileDataFetching;
import graphql.PublicApi;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;

/**
 * This graphql error will be used if a runtime exception is encountered while a data fetcher is invoked
 */
@PublicApi
public class DefaultGraphQLError extends ExceptionWhileDataFetching {

    public DefaultGraphQLError(ResultPath path, Throwable exception, SourceLocation sourceLocation) {
        super(path, exception, sourceLocation);
    }

    @Override
    public String toString() {
        return "DefaultGraphQLError{" +
            "path=" + getPath() +
            ", locations=" + getLocations() +
            '}';
    }
}
