package io.kgraph.kgraphql.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

public class EntityFetcher implements DataFetcher {

    private final GraphQLQueryFactory queryFactory;

    public EntityFetcher(GraphQLQueryFactory queryFactory) {
        this.queryFactory = queryFactory;
    }

    @Override
    public Object get(DataFetchingEnvironment environment) {
        return queryFactory.queryResult(environment);
    }
}
