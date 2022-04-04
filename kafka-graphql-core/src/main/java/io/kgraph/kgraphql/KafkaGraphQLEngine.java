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
import io.kcache.Cache;
import io.kgraph.kgraphql.schema.GraphQLExecutor;
import io.kgraph.kgraphql.schema.GraphQLSchemaBuilder;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaGraphQLEngine implements Configurable, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaGraphQLEngine.class);

    private KafkaGraphQLConfig config;
    private GraphQLExecutor executor;
    private Cache<Long, Long> cache;
    private final AtomicBoolean initialized = new AtomicBoolean();

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
    }

    public void configure(Map<String, ?> configs) {
        configure(new KafkaGraphQLConfig(configs));
    }

    public void configure(KafkaGraphQLConfig config) {
        this.config = config;
    }

    public void init() {
        GraphQLSchemaBuilder schemaBuilder = new GraphQLSchemaBuilder();
        executor = new GraphQLExecutor(config, null, schemaBuilder);

        boolean isInitialized = initialized.compareAndSet(false, true);
        if (!isInitialized) {
            throw new IllegalStateException("Illegal state while initializing engine. Engine "
                + "was already initialized");
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

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.close();
        }
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
