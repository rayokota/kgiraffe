package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class EntityFetcher implements DataFetcher {

    private final GraphQLQueryFactory queryFactory;

    public EntityFetcher(GraphQLQueryFactory queryFactory) {
        this.queryFactory = queryFactory;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        return queryFactory.queryResult(env);
    }
}
