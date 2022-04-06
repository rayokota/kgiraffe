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
package io.kgraph.kgraphql;

import graphql.GraphQL;
import io.hdocdb.store.HDocumentDB;
import io.hdocdb.store.InMemoryHDocumentDB;
import io.kcache.Cache;
import io.kcache.CacheUpdateHandler;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;
import io.kcache.caffeine.CaffeineCache;
import io.kgraph.kgraphql.schema.GraphQLExecutor;
import io.kgraph.kgraphql.schema.GraphQLSchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.ojai.Document;
import org.ojai.json.Json;
import org.ojai.store.DocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class KafkaGraphQLEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaGraphQLEngine.class);

    private KafkaGraphQLConfig config;
    private GraphQLExecutor executor;
    private Map<String, Cache<byte[], byte[]>> caches;
    private HDocumentDB docdb;
    private final AtomicBoolean initialized;

    private static KafkaGraphQLEngine INSTANCE;

    public synchronized static KafkaGraphQLEngine getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new KafkaGraphQLEngine();
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

    private KafkaGraphQLEngine() {
        try {
            caches = new HashMap<>();
            docdb = new InMemoryHDocumentDB();
            initialized = new AtomicBoolean();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void configure(Map<String, ?> configs) {
        configure(new KafkaGraphQLConfig(configs));
    }

    public void configure(KafkaGraphQLConfig config) {
        this.config = config;
    }

    public void init() {
        List<String> urls = config.getSchemaRegistryUrls();
        List<String> topics = config.getTopics();
        List<SchemaProvider> providers = Arrays.asList(
            new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
        );
        SchemaRegistryClient schemaRegistry =
            new CachedSchemaRegistryClient(urls, 1000, providers, config.originals());
        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder(this, schemaRegistry, topics);
        executor = new GraphQLExecutor(config, schemaBuilder);

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
            configs.getOrDefault(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, "kafka-graphql-1");
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_GROUP_ID_CONFIG, groupId);
        configs.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, groupId + "-" + topic);
        configs.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        Cache<byte[], byte[]> cache = new KafkaCache<>(
            new KafkaCacheConfig(configs),
            Serdes.ByteArray(),
            Serdes.ByteArray(),
            new UpdateHandler(schemaRegistry),
            new CaffeineCache<>(null)
        );
        cache.init();
        caches.put(topic, cache);

        docdb.createCollection(topic);
    }

    class UpdateHandler implements CacheUpdateHandler<byte[], byte[]> {

        private SchemaRegistryClient schemaRegistry;

        public UpdateHandler(SchemaRegistryClient schemaRegistry) {
            this.schemaRegistry = schemaRegistry;
        }

        public void handleUpdate(byte[] key, byte[] value, byte[] oldValue,
                                 TopicPartition tp, long offset, long timestamp) {
            try {
                String topic = tp.topic();
                DocumentStore store = docdb.getCollection(topic);
                GenericRecord record = (GenericRecord)
                    new KafkaAvroDeserializer(schemaRegistry).deserialize(topic, value);
                byte[] json = AvroSchemaUtils.toJson(record);
                Document doc =
                    Json.newDocumentStream(new ByteArrayInputStream(json)).iterator().next();
                if (doc.getId() == null) {
                    doc.setId(UUID.randomUUID().toString());
                }
                store.insert(doc);
                store.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public void sync() {
        // TODO
        /*
        CompletableFuture<Void> commitsFuture = CompletableFuture.runAsync(() -> commits.sync());
        CompletableFuture<Void> timestampsFuture = CompletableFuture.runAsync(() ->
            timestamps.sync()).thenRunAsync(() -> transactionManager.init());
        CompletableFuture<Void> leasesFuture = CompletableFuture.runAsync(() -> leases.sync());
        CompletableFuture<Void> authFuture = CompletableFuture.runAsync(() -> auth.sync());
        CompletableFuture<Void> authUsersFuture = CompletableFuture.runAsync(() -> authUsers.sync());
        CompletableFuture<Void> authRolesFuture = CompletableFuture.runAsync(() -> authRoles.sync());
        CompletableFuture<Void> kvFuture = CompletableFuture.runAsync(() -> cache.sync());
        CompletableFuture.allOf(commitsFuture, timestampsFuture, leasesFuture,
            authFuture, authUsersFuture, authRolesFuture, kvFuture).join();

         */
    }

    public GraphQL getGraphQL() {
        return executor.getGraphQL();
    }

    public DocumentStore getCollection(String name) {
        return docdb.getCollection(name);
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
