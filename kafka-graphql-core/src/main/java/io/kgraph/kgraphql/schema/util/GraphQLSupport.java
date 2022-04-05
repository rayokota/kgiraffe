package io.kgraph.kgraphql.schema.util;

import graphql.language.Argument;
import io.kgraph.kgraphql.schema.GraphQLSchemaBuilder;

public class GraphQLSupport {

    public static boolean isWhereArgument(Argument argument) {
        return GraphQLSchemaBuilder.WHERE_PARAM_NAME.equals(argument.getName());
    }

    public static boolean isLimitArgument(Argument argument) {
        return GraphQLSchemaBuilder.LIMIT_PARAM_NAME.equals(argument.getName());
    }

    public static boolean isOffsetArgument(Argument argument) {
        return GraphQLSchemaBuilder.OFFSET_PARAM_NAME.equals(argument.getName());
    }

    public static boolean isOrderByArgument(Argument argument) {
        return GraphQLSchemaBuilder.ORDER_BY_PARAM_NAME.equals(argument.getName());
    }
}
