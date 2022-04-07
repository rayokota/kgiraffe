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

import graphql.GraphQL;
import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.hdocdb.store.InMemoryHDocumentDB;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kgraph.kgiraffe.schema.GraphQLExecutor;
import io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder;
import io.kgraph.kgiraffe.util.KryoCodec;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.rxjava3.core.eventbus.EventBus;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.ojai.Document;
import org.ojai.json.Json;
import org.ojai.types.ODate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.OFFSET_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PARTITION_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TIMESTAMP_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.TOPIC_ATTR_NAME;

public class KGiraffeEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeEngine.class);

    private KGiraffeConfig config;
    private EventBus eventBus;
    private GraphQLExecutor executor;
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
            throw new RuntimeException(e);
        }
    }

    public void configure(Map<String, ?> configs) {
        configure(new KGiraffeConfig(configs));
    }

    public void configure(KGiraffeConfig config) {
        this.config = config;
    }

    public void init(EventBus eventBus) {
        this.eventBus = eventBus.registerCodec(new KryoCodec<Document>());

        List<String> urls = config.getSchemaRegistryUrls();
        List<String> topics = config.getTopics();
        List<SchemaProvider> providers = Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
        );
        SchemaRegistryClient schemaRegistry =
            new CachedSchemaRegistryClient(urls, 1000, providers, config.originals());
        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder(this, schemaRegistry, topics);
        this.executor = new GraphQLExecutor(config, schemaBuilder);

        initTopics(schemaRegistry);

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new IllegalStateException("Illegal state while initializing engine. Engine "
                + "was already initialized");
        }
    }

    private void initTopics(SchemaRegistryClient schemaRegistry) {
        for (String topic : config.getTopics()) {
            initTopic(schemaRegistry, topic);
        }
    }

    private void initTopic(SchemaRegistryClient schemaRegistry, String topic) {
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

        public void handleUpdate(Bytes key, Bytes value, Bytes oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
            try {
                String topic = tp.topic();
                int partition = tp.partition();
                String id = topic + "-" + partition + "-" + offset;
                HDocumentCollection coll = docdb.getCollection(topic);
                GenericRecord record = (GenericRecord)
                    new KafkaAvroDeserializer(schemaRegistry).deserialize(topic, value.get());
                // TODO
                byte[] keyBytes = null;
                byte[] valueBytes = AvroSchemaUtils.toJson(record);

                Document doc = Json.newDocumentStream(
                    new ByteArrayInputStream(valueBytes)).iterator().next();
                doc.setId(id);
                doc.set(TOPIC_ATTR_NAME, topic);
                doc.set(PARTITION_ATTR_NAME, partition);
                doc.set(OFFSET_ATTR_NAME, offset);
                doc.set(TIMESTAMP_ATTR_NAME, timestamp);
                coll.insertOrReplace(doc);
                coll.flush();
                doc = coll.findById(id);

                DeliveryOptions options = new DeliveryOptions().setCodecName("kryo");
                eventBus.publish(topic, doc, options);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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

    public EventBus getEventBus() {
        return eventBus;
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
