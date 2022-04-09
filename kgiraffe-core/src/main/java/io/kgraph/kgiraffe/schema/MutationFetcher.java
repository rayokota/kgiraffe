package io.kgraph.kgiraffe.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Map;
import java.util.Optional;

import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.kcache.KafkaCache;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.ojai.Document;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VALUE_ATTR_NAME;

public class MutationFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(MutationFetcher.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final KGiraffeEngine engine;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final Either<Type, ParsedSchema> valueSchema;

    public MutationFetcher(KGiraffeEngine engine,
                           String topic,
                           Either<Type, ParsedSchema> keySchema,
                           Either<Type, ParsedSchema> valueSchema) {
        this.engine = engine;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Object key = env.getArgument(KEY_ATTR_NAME);
            Object value = env.getArgument(VALUE_ATTR_NAME);

            Bytes keyBytes = key != null ? Bytes.wrap(engine.serializeKey(topic, key)) : null;
            Bytes valueBytes = Bytes.wrap(engine.serializeValue(topic, value));

            KafkaCache<Bytes, Bytes> cache = engine.getCache(topic);
            // TODO key
            // TODO header
            RecordMetadata metadata = cache.put(null, keyBytes, valueBytes).getRecordMetadata();
            String id = metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset();

            HDocumentDB docdb = engine.getDocDB();
            HDocumentCollection coll = docdb.get(topic);
            Document doc = coll.findById(id);
            return doc;
        } catch (Exception e) {
            LOG.error("Error during mutate", e);
            throw new RuntimeException(e);
        }
    }
}
