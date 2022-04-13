package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.ojai.Document;
import org.ojai.Value;

public class AttributeFetcher implements DataFetcher {

    private final String attributeName;

    public AttributeFetcher(
        String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        Document entity = env.getSource();
        String attrName = env.getField().getName();
        Value value = entity.getValue(attrName);
        return value != null ? value.getObject() : null;
    }
}
