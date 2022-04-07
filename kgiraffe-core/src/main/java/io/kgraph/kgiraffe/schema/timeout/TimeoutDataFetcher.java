package io.kgraph.kgiraffe.schema.timeout;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class TimeoutDataFetcher<T> implements DataFetcher<T> {

    private long maxDuration;

    public TimeoutDataFetcher(long maxDuration) {
        this.maxDuration = maxDuration;
    }

    @Override
    public T get(DataFetchingEnvironment environment) throws Exception {
        throw new QueryTimeoutException("Maximum query duration of " + maxDuration + " ms exceeded.");
    }
}
