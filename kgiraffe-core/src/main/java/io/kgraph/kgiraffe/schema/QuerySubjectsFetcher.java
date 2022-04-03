package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.kgraph.kgiraffe.KGiraffeEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.SUBJECT_PREFIX_PARAM_NAME;

public class QuerySubjectsFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(QuerySubjectsFetcher.class);

    private final KGiraffeEngine engine;

    public QuerySubjectsFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            String subjectPrefix = env.getArgument(SUBJECT_PREFIX_PARAM_NAME);
            return engine.getSchemaRegistry().getAllSubjectsByPrefix(subjectPrefix);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
