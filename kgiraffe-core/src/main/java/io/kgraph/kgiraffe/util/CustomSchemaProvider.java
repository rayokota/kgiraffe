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

// TODO remove once CP 7.2.0 is released
public class CustomSchemaProvider extends AbstractSchemaProvider {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSchemaProvider.class);

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
                throw new IllegalArgumentException("Illegal schema type " + schemaType);
        }
    }
}
