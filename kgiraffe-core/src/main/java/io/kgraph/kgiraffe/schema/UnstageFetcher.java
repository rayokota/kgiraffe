package io.kgraph.kgiraffe.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.util.Jackson;
import io.vavr.Tuple2;
import org.ojai.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.ID_ATTR_NAME;

public class UnstageFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(UnstageFetcher.class);

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    private final KGiraffeEngine engine;

    public UnstageFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Integer id = env.getArgument(ID_ATTR_NAME);
            Tuple2<Document, Optional<ParsedSchema>> optSchema = engine.unstageSchema(id);
            if (optSchema._2.isEmpty()) {
                throw new IllegalArgumentException("Could not unstage schema with id " + id);
            }
            return optSchema._1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
