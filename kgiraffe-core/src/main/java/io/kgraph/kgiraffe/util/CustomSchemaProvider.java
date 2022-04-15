/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kgraph.kgiraffe.util;

import io.kgraph.kgiraffe.KGiraffeEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaVersionFetcher;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

public class CustomSchemaProvider extends AbstractSchemaProvider {
    private static final Logger log = LoggerFactory.getLogger(CustomSchemaProvider.class);

    private final KGiraffeEngine engine;

    public CustomSchemaProvider(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public SchemaVersionFetcher schemaVersionFetcher() {
        return engine.getSchemaRegistry();
    }

    @Override
    public String schemaType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ParsedSchema> parseSchema(String schemaString,
                                              List<SchemaReference> references,
                                              boolean isNew) {
        throw new UnsupportedOperationException();
    }

    public ParsedSchema parseSchema(String schemaType,
                                    String schemaString,
                                    List<SchemaReference> references) {
        switch (schemaType) {
            case "AVRO":
                return new AvroSchema(schemaString, references,
                    resolveReferences(references), null, true);
            case "JSON":
                return new JsonSchema(schemaString, references,
                    resolveReferences(references), null);
            case "PROTOBUF":
                return new ProtobufSchema(schemaString, references,
                    resolveReferences(references), null, null);
            default:
                throw new IllegalArgumentException("Unsupported schema type " + schemaType);
        }
    }
}
