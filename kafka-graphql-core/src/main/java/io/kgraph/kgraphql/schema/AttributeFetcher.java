package io.kgraph.kgraphql.schema;

import graphql.GraphQLContext;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;
import org.ojai.Document;
import org.ojai.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;

public class AttributeFetcher implements DataFetcher {

    private final String attributeName;

    public AttributeFetcher(
        String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object get(DataFetchingEnvironment environment) {
        Document entity = environment.getSource();
        String attrName = environment.getField().getName();
        if (attrName.equals(KEY_ATTR_NAME)) {
            // TODO
            return null;
        }

        return entity.getValue(attrName);
    }
}
