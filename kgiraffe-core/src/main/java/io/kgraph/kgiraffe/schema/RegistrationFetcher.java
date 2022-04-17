package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.vavr.Tuple2;
import org.ojai.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.ID_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.NORMALIZE_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.SUBJECT_ATTR_NAME;

public class RegistrationFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(RegistrationFetcher.class);

    private final KGiraffeEngine engine;

    public RegistrationFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Integer id = env.getArgument(ID_ATTR_NAME);
            String subject = env.getArgument(SUBJECT_ATTR_NAME);
            boolean normalize = env.getArgumentOrDefault(NORMALIZE_PARAM_NAME, false);
            Tuple2<Document, Optional<ParsedSchema>> optSchema =
                engine.registerSchema(subject, id, normalize);
            if (optSchema._2.isEmpty()) {
                throw new IllegalArgumentException("Could not register schema with id " + id);
            }
            return optSchema._1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
