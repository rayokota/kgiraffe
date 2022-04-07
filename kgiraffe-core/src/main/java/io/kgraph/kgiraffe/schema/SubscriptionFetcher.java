package io.kgraph.kgiraffe.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.store.HQueryCondition;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.reactivex.rxjava3.core.Flowable;
import io.vavr.control.Either;
import org.ojai.Document;
import org.ojai.Value.Type;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class SubscriptionFetcher implements DataFetcher {

    private final KGiraffeEngine engine;
    private final SchemaRegistryClient schemaRegistry;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;
    private final GraphQLQueryFactory queryFactory;

    public SubscriptionFetcher(KGiraffeEngine engine,
                               SchemaRegistryClient schemaRegistry,
                               String topic,
                               Either<Type, ParsedSchema> keySchema,
                               ParsedSchema valueSchema,
                               GraphQLQueryFactory queryFactory) {
        this.engine = engine;
        this.schemaRegistry = schemaRegistry;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.queryFactory = queryFactory;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            HQueryCondition query = queryFactory.getCriteriaQuery(env, env.getField());
            Flowable<Document> publisher = engine.getEventBus()
                .consumer(topic)
                .toFlowable()
                .map(m -> (Document) m.body())
                .filter(doc -> query == null || query.isEmpty() || query.evaluate(doc));
            return publisher;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
