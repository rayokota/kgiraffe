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

import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.kcache.KafkaCache;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.vavr.control.Either;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Bytes;
import org.ojai.Document;
import org.ojai.Value.Type;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VALUE_ATTR_NAME;

public class MutationFetcher implements DataFetcher {

    private final static ObjectMapper MAPPER = new ObjectMapper();

    private final KGiraffeEngine engine;
    private final SchemaRegistryClient schemaRegistry;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;

    public MutationFetcher(KGiraffeEngine engine,
                           SchemaRegistryClient schemaRegistry,
                           String topic,
                           Either<Type, ParsedSchema> keySchema,
                           ParsedSchema valueSchema) {
        this.engine = engine;
        this.schemaRegistry = schemaRegistry;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            // TODO
            Map<String, Object> key = env.getArgument(KEY_ATTR_NAME);
            Map<String, Object> value = env.getArgument(VALUE_ATTR_NAME);

            JsonNode json = MAPPER.valueToTree(value);
            Object valueObj = AvroSchemaUtils.toObject(json, (AvroSchema) valueSchema);

            Bytes keyBytes = Bytes.wrap(Bytes.EMPTY);
            Bytes valueBytes = Bytes.wrap(
                new KafkaAvroSerializer(schemaRegistry).serialize(topic, valueObj));
            KafkaCache<Bytes, Bytes> cache = engine.getCache(topic);
            // TODO key
            RecordMetadata metadata = cache.put(null, null, valueBytes).getRecordMetadata();
            String id = metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset();

            HDocumentDB docdb = engine.getDocDB();
            HDocumentCollection coll = docdb.get(topic);
            Document doc = coll.findById(id);
            return doc;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
