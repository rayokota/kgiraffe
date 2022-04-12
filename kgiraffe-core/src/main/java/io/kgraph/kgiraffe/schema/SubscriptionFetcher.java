package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.store.HQueryCondition;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.reactivex.rxjava3.core.Flowable;
import io.vavr.control.Either;
import org.ojai.Document;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ParsedSchema;

public class SubscriptionFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionFetcher.class);

    private final KGiraffeEngine engine;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final Either<Type, ParsedSchema> valueSchema;
    private final GraphQLQueryFactory queryFactory;

    public SubscriptionFetcher(KGiraffeEngine engine,
                               String topic,
                               Either<Type, ParsedSchema> keySchema,
                               Either<Type, ParsedSchema> valueSchema,
                               GraphQLQueryFactory queryFactory) {
        this.engine = engine;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.queryFactory = queryFactory;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            HQueryCondition query = queryFactory.getCriteriaQuery(env, env.getField());
            Flowable<Document> publisher = engine.getNotifier()
                .consumer(topic)
                .filter(doc -> query == null || query.isEmpty() || query.evaluate(doc));
            return publisher;
        } catch (Exception e) {
            LOG.error("Error during subscribe", e);
            throw new RuntimeException(e);
        }
    }
}
