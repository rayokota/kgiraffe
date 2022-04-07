package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.ojai.Document;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;

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
