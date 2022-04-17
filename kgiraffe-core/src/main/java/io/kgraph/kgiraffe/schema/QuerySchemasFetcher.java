package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.vavr.Tuple2;
import org.ojai.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.ID_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.SUBJECT_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VERSION_ATTR_NAME;

public class QuerySchemasFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(QuerySchemasFetcher.class);

    private final KGiraffeEngine engine;

    public QuerySchemasFetcher(KGiraffeEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Integer id = env.getArgument(ID_ATTR_NAME);
            String subject = env.getArgument(SUBJECT_ATTR_NAME);
            Integer version = env.getArgument(VERSION_ATTR_NAME);

            List<Document> docs = new ArrayList<>();
            if (id != null) {
                Tuple2<Document, Optional<ParsedSchema>> optSchema = engine.getSchemaById(id);
                if (optSchema._2.isEmpty()) {
                    throw new IllegalArgumentException("Could not find schema with id " + id);
                }
                Document doc = optSchema._1;
                docs.add(doc);
            } else if (subject != null && version != null) {
                Document doc = querySchema(subject, version);
                docs.add(doc);
            } else if (subject != null) {
                List<Integer> versions = engine.getSchemaRegistry().getAllVersions(subject);
                for (int v : versions) {
                    Document doc = querySchema(subject, v);
                    docs.add(doc);
                }
            } else {
                throw new IllegalArgumentException("Missing id or subject");
            }
            return docs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Document querySchema(String subject, int version) {
        Tuple2<Document, Optional<ParsedSchema>> optSchema =
            engine.getSchemaByVersion(subject, version);
        if (optSchema._2.isEmpty()) {
            throw new IllegalArgumentException("Could not find schema with subject " + subject
                + ", version " + version);
        }
        return optSchema._1;
    }
}
