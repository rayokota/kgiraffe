package io.kgraph.kgiraffe.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.util.Jackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.REFERENCES_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.SCHEMA_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.SCHEMA_TYPE_ATTR_NAME;

public class StageFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(StageFetcher.class);

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    private final KGiraffeEngine engine;

    public StageFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            String schemaType = env.getArgument(SCHEMA_TYPE_ATTR_NAME);
            String schema = env.getArgument(SCHEMA_ATTR_NAME);
            List<Map<String, Object>> list =
                env.getArgumentOrDefault(REFERENCES_ATTR_NAME, Collections.emptyList());

            List<SchemaReference> refs = MAPPER.convertValue(list, new TypeReference<>() {
            });
            return engine.stageSchemas(schemaType, schema, refs)._1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
