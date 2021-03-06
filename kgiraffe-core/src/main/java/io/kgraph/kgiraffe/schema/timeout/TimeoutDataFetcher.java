package io.kgraph.kgiraffe.schema.timeout;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class TimeoutDataFetcher<T> implements DataFetcher<T> {

    private final long maxDuration;

    public TimeoutDataFetcher(long maxDuration) {
        this.maxDuration = maxDuration;
    }

    @Override
    public T get(DataFetchingEnvironment env) throws Exception {
        throw new QueryTimeoutException("Maximum query duration of " + maxDuration + " ms exceeded.");
    }
}
