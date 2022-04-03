package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.HDocument;
import io.kgraph.kgiraffe.KGiraffeEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.IS_BACKWARD_COMPATIBLE_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.MESSAGES_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.NEXT_ID_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PREV_ID_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PREV_SUBJECT_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.PREV_VERSION_PARAM_NAME;

public class CompatibilityFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(CompatibilityFetcher.class);

    private final KGiraffeEngine engine;

    public CompatibilityFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Integer nextSchemaId = env.getArgument(NEXT_ID_PARAM_NAME);
            Integer prevSchemaId = env.getArgument(PREV_ID_PARAM_NAME);
            String prevSubject = env.getArgument(PREV_SUBJECT_PARAM_NAME);
            int prevVersion = env.getArgumentOrDefault(PREV_VERSION_PARAM_NAME, -1);
            if (nextSchemaId == null) {
                throw new IllegalArgumentException("Missing next schema id");
            }
            if (prevSchemaId == null && prevSubject == null) {
                throw new IllegalArgumentException("Missing previous schema id or subject");
            }

            Optional<ParsedSchema> nextSchema = engine.getSchemaById(nextSchemaId)._2;
            Optional<ParsedSchema> prevSchema;
            if (prevSchemaId != null) {
                prevSchema = engine.getSchemaById(prevSchemaId)._2;
            } else {
                prevSchema = engine.getSchemaByVersion(prevSubject, prevVersion)._2;
            }

            HDocument doc = new HDocument();
            if (prevSchema.isEmpty()) {
                doc.set(IS_BACKWARD_COMPATIBLE_ATTR_NAME, false);
                doc.set(MESSAGES_ATTR_NAME,
                    Collections.singletonList("Could not parse previous schema"));
                return doc;
            }
            if (nextSchema.isEmpty()) {
                doc.set(IS_BACKWARD_COMPATIBLE_ATTR_NAME, false);
                doc.set(MESSAGES_ATTR_NAME,
                    Collections.singletonList("Could not parse next schema with id " + nextSchemaId));
                return doc;
            }

            List<String> errors = nextSchema.get().isBackwardCompatible(prevSchema.get());
            if (errors.isEmpty()) {
                doc.set(IS_BACKWARD_COMPATIBLE_ATTR_NAME, true);
                return doc;
            } else {
                doc.set(IS_BACKWARD_COMPATIBLE_ATTR_NAME, false);
                doc.set(MESSAGES_ATTR_NAME, errors);
                return doc;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
