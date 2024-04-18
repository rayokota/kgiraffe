package io.kgraph.kgiraffe.schema.util;

import graphql.GraphQLError;
import graphql.PublicApi;
import graphql.execution.DataFetcherExceptionHandlerParameters;
import graphql.execution.DataFetcherExceptionHandlerResult;
import graphql.execution.ResultPath;
import graphql.execution.SimpleDataFetcherExceptionHandler;
import graphql.language.SourceLocation;

import java.util.concurrent.CompletableFuture;

/**
 * The standard handling of data fetcher error involves placing a {@link DefaultGraphQLError} error
 * into the error collection
 */
@PublicApi
public class DefaultDataFetcherExceptionHandler extends SimpleDataFetcherExceptionHandler {

    private DataFetcherExceptionHandlerResult handleExceptionImpl(DataFetcherExceptionHandlerParameters handlerParameters) {
        Throwable exception = unwrap(handlerParameters.getException());
        SourceLocation sourceLocation = handlerParameters.getSourceLocation();
        ResultPath path = handlerParameters.getPath();

        DefaultGraphQLError error = new DefaultGraphQLError(path, exception, sourceLocation);
        logException(error, exception);

        return DataFetcherExceptionHandlerResult.newResult().error(error).build();
    }

    @Override
    public CompletableFuture<DataFetcherExceptionHandlerResult> handleException(DataFetcherExceptionHandlerParameters handlerParameters) {
        return CompletableFuture.completedFuture(handleExceptionImpl(handlerParameters));
    }
}
