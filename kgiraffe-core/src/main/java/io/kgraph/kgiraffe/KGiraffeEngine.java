/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.kgiraffe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import graphql.GraphQL;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.hdocdb.store.InMemoryHDocumentDB;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kgraph.kgiraffe.notifier.Notifier;
import io.kgraph.kgiraffe.schema.GraphQLExecutor;
import io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder;
import io.kgraph.kgiraffe.schema.converters.GraphQLProtobufConverter;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.ojai.Document;
import org.ojai.Value;
import org.ojai.Value.Type;
import org.ojai.json.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.EPOCH_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.HEADERS_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ERROR_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.OFFSET_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PARTITION_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TIMESTAMP_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TIMESTAMP_TYPE_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TOPIC_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VALUE_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VALUE_ERROR_ATTR_NAME;

public class KGiraffeEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeEngine.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KGiraffeConfig config;
    private Notifier notifier;
    private SchemaRegistryClient schemaRegistry;
    private GraphQLExecutor executor;
    private Map<String, KGiraffeConfig.Serde> keySerdes;
    private Map<String, KGiraffeConfig.Serde> valueSerdes;
    private Map<String, Either<Type, ParsedSchema>> keySchemas = new HashMap<>();
    private Map<String, Either<Type, ParsedSchema>> valueSchemas = new HashMap<>();
    private Map<Tuple2<String, ProtobufSchema>, ProtobufSchema> schemaCache = new HashMap<>();
    private Map<String, KafkaCache<Bytes, Bytes>> caches;
    private HDocumentDB docdb;
    private final AtomicBoolean initialized;

    private static KGiraffeEngine INSTANCE;

    public synchronized static KGiraffeEngine getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new KGiraffeEngine();
        }
        return INSTANCE;
    }

    public synchronized static void closeInstance() {
        if (INSTANCE != null) {
            try {
                INSTANCE.close();
            } catch (IOException e) {
                LOG.warn("Could not close engine", e);
            }
            INSTANCE = null;
        }
    }

    private KGiraffeEngine() {
        try {
            caches = new HashMap<>();
            docdb = new InMemoryHDocumentDB();
            initialized = new AtomicBoolean();
        } catch (IOException e) {
            LOG.error("Error during startup", e);
            throw new RuntimeException(e);
        }
    }

    public void configure(Map<String, ?> configs) {
        configure(new KGiraffeConfig(configs));
    }

    public void configure(KGiraffeConfig config) {
        this.config = config;
    }

    public void init(Notifier notifier) {
        this.notifier = notifier;

        List<String> urls = config.getSchemaRegistryUrls();
        List<String> topics = config.getTopics();
        keySerdes = config.getKeySerdes();
        valueSerdes = config.getValueSerdes();
        List<SchemaProvider> providers = Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
        );
        schemaRegistry = createSchemaRegistry(urls, providers);
        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder(this, topics);
        this.executor = new GraphQLExecutor(config, schemaBuilder);

        initCaches();

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new IllegalStateException("Illegal state while initializing engine. Engine "
                + "was already initialized");
        }
    }

    private SchemaRegistryClient createSchemaRegistry(List<String> urls,
                                                      List<SchemaProvider> providers) {
        String mockScope = MockSchemaRegistry.validateAndMaybeGetMockScope(urls);
        if (mockScope != null) {
            return MockSchemaRegistry.getClientForScope(mockScope, providers);
        } else {
            return new CachedSchemaRegistryClient(urls, 1000, providers, config.originals());
        }
    }

    public Notifier getNotifier() {
        return notifier;
    }

    public SchemaRegistryClient getSchemaRegistry() {
        return schemaRegistry;
    }

    public Either<Type, ParsedSchema> getKeySchema(String topic) {
        return keySchemas.computeIfAbsent(topic, t -> getSchema(topic + "-key",
            keySerdes.getOrDefault(topic, KGiraffeConfig.Serde.KEY_DEFAULT)));
    }

    public Either<Type, ParsedSchema> getValueSchema(String topic) {
        return valueSchemas.computeIfAbsent(topic, t -> getSchema(topic + "-value",
            valueSerdes.getOrDefault(topic, KGiraffeConfig.Serde.VALUE_DEFAULT)));
    }

    private Either<Type, ParsedSchema> getSchema(String subject, KGiraffeConfig.Serde serde) {
        switch (serde.getSerdeType()) {
            case SHORT:
                return Either.left(Type.SHORT);
            case INT:
                return Either.left(Type.INT);
            case LONG:
                return Either.left(Type.LONG);
            case FLOAT:
                return Either.left(Type.FLOAT);
            case DOUBLE:
                return Either.left(Type.DOUBLE);
            case STRING:
                return Either.left(Type.STRING);
            case BINARY:
                return Either.left(Type.BINARY);
            case LATEST:
                return getLatestSchema(subject).<Either<Type, ParsedSchema>>map(Either::right)
                    .orElseGet(() -> {
                        LOG.error("Could not find latest schema for subject {}, "
                            + "defaulting to \"binary\"", subject);
                        return Either.left(Type.BINARY);
                    });
            case ID:
                return getSchemaById(serde.getId()).<Either<Type, ParsedSchema>>map(Either::right)
                    .orElseGet(() -> {
                        LOG.error("Could not find schema with id {}, "
                            + "defaulting to \"binary\"", serde.getId());
                        return Either.left(Type.BINARY);
                    });
            default:
                throw new IllegalArgumentException("Illegal serde type: " + serde.getSerdeType());
        }
    }

    private Optional<ParsedSchema> getLatestSchema(String subject) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            Optional<ParsedSchema> optSchema =
                schemaRegistry.parseSchema(
                    schemaMetadata.getSchemaType(),
                    schemaMetadata.getSchema(),
                    schemaMetadata.getReferences());
            return optSchema;
        } catch (IOException | RestClientException e) {
            return Optional.empty();
        }
    }

    private Optional<ParsedSchema> getSchemaById(int id) {
        try {
            return Optional.of(schemaRegistry.getSchemaById(id));
        } catch (IOException | RestClientException e) {
            return Optional.empty();
        }
    }

    public Value deserializeKey(String topic, byte[] bytes) throws IOException {
        return deserialize(getKeySchema(topic), topic, bytes);
    }

    public Value deserializeValue(String topic, byte[] bytes) throws IOException {
        return deserialize(getValueSchema(topic), topic, bytes);
    }

    private Value deserialize(Either<Type, ParsedSchema> schema,
                              String topic,
                              byte[] bytes) throws IOException {
        Deserializer<?> deserializer = getDeserializer(schema);

        Object object = deserializer.deserialize(topic, bytes);
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            byte[] json;
            String typeName = null;
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    json = AvroSchemaUtils.toJson(object);
                    break;
                case "JSON":
                    json = JsonSchemaUtils.toJson(object);
                    break;
                case "PROTOBUF":
                    ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema;
                    Message message = (Message) object;
                    json = ProtobufSchemaUtils.toJson(message);
                    if (GraphQLProtobufConverter.hasMultipleMessageTypes(protobufSchema)) {
                        typeName = message.getDescriptorForType().getName();
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
            Document doc = Json.newDocumentStream(new ByteArrayInputStream(json)).iterator().next();
            if (typeName != null) {
                HDocument rootDoc = new HDocument();
                rootDoc.set(typeName, doc);
                doc = rootDoc;
            }
            return HValue.initFromDocument(doc);
        } if (schema.getLeft() == Type.BINARY) {
            object = Base64.getEncoder().encodeToString(((Bytes) object).get());
        }

        return HValue.initFromObject(object);
    }

    public byte[] serializeKey(String topic, Object object) throws IOException {
        return serialize(getKeySchema(topic), topic, object);
    }

    public byte[] serializeValue(String topic, Object object) throws IOException {
        return serialize(getValueSchema(topic), topic, object);
    }

    private byte[] serialize(Either<Type, ParsedSchema> schema,
                             String topic,
                             Object object) throws IOException {
        Serializer<Object> serializer = (Serializer<Object>) getSerializer(schema);

        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            JsonNode json = MAPPER.convertValue(object, JsonNode.class);
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    object = AvroSchemaUtils.toObject(json, (AvroSchema) parsedSchema);
                    break;
                case "JSON":
                    object = JsonSchemaUtils.toObject(json, (JsonSchema) parsedSchema);
                    break;
                case "PROTOBUF":
                    ProtobufSchema protobufSchema = (ProtobufSchema) parsedSchema;
                    if (GraphQLProtobufConverter.hasMultipleMessageTypes(protobufSchema)) {
                        String typeName = json.fieldNames().next();
                        json = json.get(typeName);
                        protobufSchema = schemaWithName(protobufSchema, typeName);
                    }
                    object = ProtobufSchemaUtils.toObject(json, protobufSchema);
                    break;
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
        } else if (schema.getLeft() == Type.BINARY) {
            object = Base64.getDecoder().decode((String) object);
        }

        return serializer.serialize(topic, object);
    }

    private ProtobufSchema schemaWithName(ProtobufSchema schema, String name) {
        return schemaCache.computeIfAbsent(new Tuple2<>(name, schema), k -> schema.copy(name));
    }

    public Serializer<?> getSerializer(Either<Type, ParsedSchema> schema) {
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    return new KafkaAvroSerializer(schemaRegistry);
                case "JSON":
                    return new KafkaJsonSchemaSerializer<>(schemaRegistry);
                case "PROTOBUF":
                    return new KafkaProtobufSerializer<>(schemaRegistry);
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
        } else {
            switch (schema.getLeft()) {
                case STRING:
                    return new StringSerializer();
                case SHORT:
                    return new ShortSerializer();
                case INT:
                    return new IntegerSerializer();
                case LONG:
                    return new LongSerializer();
                case FLOAT:
                    return new FloatSerializer();
                case DOUBLE:
                    return new DoubleSerializer();
                case BINARY:
                    return new BytesSerializer();
                default:
                    throw new IllegalArgumentException("Illegal type " + schema.getLeft());
            }
        }
    }

    public Deserializer<?> getDeserializer(Either<Type, ParsedSchema> schema) {
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    return new KafkaAvroDeserializer(schemaRegistry);
                case "JSON":
                    return new KafkaJsonSchemaDeserializer<>(schemaRegistry);
                case "PROTOBUF":
                    return new KafkaProtobufDeserializer<>(schemaRegistry);
                default:
                    throw new IllegalArgumentException("Illegal type " + parsedSchema.schemaType());
            }
        } else {
            switch (schema.getLeft()) {
                case STRING:
                    return new StringDeserializer();
                case SHORT:
                    return new ShortDeserializer();
                case INT:
                    return new IntegerDeserializer();
                case LONG:
                    return new LongDeserializer();
                case FLOAT:
                    return new FloatDeserializer();
                case DOUBLE:
                    return new DoubleDeserializer();
                case BINARY:
                    return new BytesDeserializer();
                default:
                    throw new IllegalArgumentException("Illegal type " + schema.getLeft());
            }
        }
    }

    private void initCaches() {
        for (String topic : config.getTopics()) {
            initCache(topic);
        }
    }

    private void initCache(String topic) {
        Map<String, Object> configs = new HashMap<>(config.originals());
        String groupId = (String)
            configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kgiraffe-1");
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        KafkaCache<Bytes, Bytes> cache = new KafkaCache<>(
            new KafkaCacheConfig(configs),
            Serdes.Bytes(),
            Serdes.Bytes(),
            new UpdateHandler(schemaRegistry),
            new CaffeineCache<>(null)
        );
        cache.init();
        caches.put(topic, cache);

        docdb.createCollection(topic);
    }

    class UpdateHandler implements CacheUpdateHandler<Bytes, Bytes> {

        private SchemaRegistryClient schemaRegistry;

        public UpdateHandler(SchemaRegistryClient schemaRegistry) {
            this.schemaRegistry = schemaRegistry;
        }

        public void handleUpdate(Headers headers,
                                 Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts, TimestampType tsType,
                                 Optional<Integer> leaderEpoch) {
            try {
                String topic = tp.topic();
                int partition = tp.partition();
                String id = topic + "-" + partition + "-" + offset;
                HDocumentCollection coll = docdb.getCollection(topic);
                Document doc = new HDocument();
                doc.setId(id);

                Map<String, Object> headersObj = convertHeaders(headers);
                if (headersObj != null) {
                    doc.set(HEADERS_ATTR_NAME, headersObj);
                }
                if (key != null && key.get() != Bytes.EMPTY) {
                    try {
                        doc.set(KEY_ATTR_NAME, deserializeKey(topic, key.get()));
                    } catch (IOException e) {
                        doc.set(KEY_ERROR_ATTR_NAME, trace(e));
                    }
                }
                try {
                    doc.set(VALUE_ATTR_NAME, deserializeValue(topic, value.get()));
                } catch (IOException e) {
                    doc.set(VALUE_ERROR_ATTR_NAME, trace(e));
                }

                doc.set(TOPIC_ATTR_NAME, topic);
                doc.set(PARTITION_ATTR_NAME, partition);
                doc.set(OFFSET_ATTR_NAME, offset);
                doc.set(TIMESTAMP_ATTR_NAME, ts);
                doc.set(TIMESTAMP_TYPE_ATTR_NAME, tsType.toString());
                if (leaderEpoch.isPresent()) {
                    doc.set(EPOCH_ATTR_NAME, leaderEpoch.get());
                }
                coll.insertOrReplace(doc);
                coll.flush();
                doc = coll.findById(id);

                notifier.publish(topic, doc);
            } catch (Exception e) {
                LOG.error("Error during update", e);
                throw new RuntimeException(e);
            }
        }

        public void handleUpdate(Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long ts) {
            throw new UnsupportedOperationException();
        }

        private Map<String, Object> convertHeaders(Headers headers) {
            if (headers == null) {
                return null;
            }
            Map<String, Object> map = new HashMap<>();
            for (Header header : headers) {
                String value = new String(header.value(), StandardCharsets.UTF_8);
                map.merge(header.key(), value, (oldV, v) -> {
                    if (oldV instanceof List) {
                        ((List<String>) oldV).add((String) v);
                        return oldV;
                    } else {
                        List<String> newV = new ArrayList<>();
                        newV.add((String) oldV);
                        newV.add((String) v);
                        return newV;
                    }
                });
            }
            return map;
        }

        private String trace(Throwable t) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8));
            return output.toString(StandardCharsets.UTF_8);
        }
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public void sync() {
        caches.forEach((key, value) -> {
            try {
                value.sync();
            } catch (Exception e) {
                LOG.warn("Could not sync cache for " + key);
            }
        });
    }

    public HDocumentDB getDocDB() {
        return docdb;
    }

    public KafkaCache<Bytes, Bytes> getCache(String topic) {
        return caches.get(topic);
    }

    public GraphQL getGraphQL() {
        return executor.getGraphQL();
    }

    @Override
    public void close() throws IOException {
        caches.forEach((key, value) -> {
            try {
                value.close();
            } catch (IOException e) {
                LOG.warn("Could not close cache for " + key);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public static <T> T getConfiguredInstance(String className, Map<String, ?> configs) {
        try {
            Class<T> cls = (Class<T>) Class.forName(className);
            Object o = Utils.newInstance(cls);
            if (o instanceof Configurable) {
                ((Configurable) o).configure(configs);
            }
            return cls.cast(o);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
