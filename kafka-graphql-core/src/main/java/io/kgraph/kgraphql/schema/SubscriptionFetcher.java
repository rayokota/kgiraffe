package io.kgraph.kgraphql.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.HDocument;
import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.hdocdb.store.HQueryCondition;
import io.kcache.KafkaCache;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.reactivex.rxjava3.core.Flowable;
import io.vavr.control.Either;
import io.vertx.rxjava3.core.eventbus.Message;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;
import org.ojai.Document;
import org.ojai.Value.Type;
import org.ojai.store.QueryCondition;
import org.reactivestreams.Publisher;

import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.KEY_PARAM_NAME;
import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.VALUE_PARAM_NAME;

public class SubscriptionFetcher implements DataFetcher {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    private final KafkaGraphQLEngine engine;
    private final SchemaRegistryClient schemaRegistry;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;
    private final GraphQLQueryFactory queryFactory;

    public SubscriptionFetcher(KafkaGraphQLEngine engine,
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
