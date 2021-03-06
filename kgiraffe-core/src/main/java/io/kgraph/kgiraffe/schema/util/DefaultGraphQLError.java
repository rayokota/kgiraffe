package io.kgraph.kgiraffe.schema.util;


import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.PublicApi;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;
import static java.lang.String.format;

/**
 * This graphql error will be used if a runtime exception is encountered while a data fetcher is invoked
 */
@PublicApi
public class DefaultGraphQLError implements GraphQLError {

    private final String message;
    private final List<Object> path;
    private final List<SourceLocation> locations;
    private final Map<String, Object> extensions;

    public DefaultGraphQLError(ResultPath path, Throwable exception, SourceLocation sourceLocation) {
        this.path = assertNotNull(path).toList();
        this.locations = Collections.singletonList(sourceLocation);
        this.extensions = mkExtensions(exception);
        this.message = mkMessage(path, exception);
    }

    private String mkMessage(ResultPath path, Throwable exception) {
        return format("Exception while fetching data (%s) : %s", path, exception.getMessage());
    }

    /*
     * This allows a DataFetcher to throw a graphql error and have "extension data" be transferred from that
     * exception into the DefaultGraphQLError error and hence have custom "extension attributes"
     * per error message.
     */
    private Map<String, Object> mkExtensions(Throwable exception) {
        Map<String, Object> extensions = null;
        if (exception instanceof GraphQLError) {
            Map<String, Object> map = ((GraphQLError) exception).getExtensions();
            if (map != null) {
                extensions = new LinkedHashMap<>(map);
            }
        }
        return extensions;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public List<SourceLocation> getLocations() {
        return locations;
    }

    @Override
    public List<Object> getPath() {
        return path;
    }

    @Override
    public Map<String, Object> getExtensions() {
        return extensions;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.DataFetchingException;
    }

    @Override
    public String toString() {
        return "DefaultGraphQLError{" +
            "path=" + path +
            ", locations=" + locations +
            '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return GraphqlErrorHelper.equals(this, o);
    }

    @Override
    public int hashCode() {
        return GraphqlErrorHelper.hashCode(this);
    }
}
