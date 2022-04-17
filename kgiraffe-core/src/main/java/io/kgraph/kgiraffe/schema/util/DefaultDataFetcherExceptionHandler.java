package io.kgraph.kgiraffe.schema.util;

import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.execution.DataFetcherExceptionHandler;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.language.SourceLocation;
import graphql.util.LogKit;
import org.slf4j.Logger;

import java.util.concurrent.CompletionException;

/**
 * The standard handling of data fetcher error involves placing a {@link DefaultGraphQLError} error
 * into the error collection
 */
@PublicApi
public class DefaultDataFetcherExceptionHandler implements DataFetcherExceptionHandler {

    private static final Logger logNotSafe = LogKit.getNotPrivacySafeLogger(DefaultDataFetcherExceptionHandler.class);

    @Override
    public DataFetcherExceptionHandlerResult onException(
        DataFetcherExceptionHandlerParameters handlerParameters) {
        Throwable exception = unwrap(handlerParameters.getException());
        SourceLocation sourceLocation = handlerParameters.getSourceLocation();
        ResultPath path = handlerParameters.getPath();

        GraphQLError error = new DefaultGraphQLError(path, exception, sourceLocation);
        logNotSafe.warn(error.getMessage(), exception);

        return DataFetcherExceptionHandlerResult.newResult().error(error).build();
    }

    /**
     * Called to unwrap an exception to a more suitable cause if required.
     *
     * @param exception the exception to unwrap
     * @return the suitable exception
     */
    protected Throwable unwrap(Throwable exception) {
        if (exception.getCause() != null) {
            if (exception instanceof CompletionException) {
                return exception.getCause();
            }
        }
        return exception;
    }
}
