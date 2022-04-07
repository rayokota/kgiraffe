package io.kgraph.kgiraffe.schema;

import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.schema.SchemaContext.Mode;
import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.everit.json.schema.ObjectSchema;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

/**
 * A wrapper for the {@link graphql.schema.GraphQLSchema.Builder}.
 */
public class GraphQLSchemaBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);

    public static final String QUERY_ROOT = "query_root";
    public static final String MUTATION_ROOT = "mutation_root";
    public static final String SUBSCRIPTION_ROOT = "subscription_root";

    public static final String LIMIT_PARAM_NAME = "limit";
    public static final String OFFSET_PARAM_NAME = "offset";
    public static final String ORDER_BY_PARAM_NAME = "order_by";
    public static final String WHERE_PARAM_NAME = "where";

    // TODO
    public static final String KEY_ATTR_NAME = "_key";
    // TODO remove _value
    public static final String VALUE_ATTR_NAME = "_value";
    public static final String TOPIC_ATTR_NAME = "_topic";
    public static final String PARTITION_ATTR_NAME = "_partition";
    public static final String OFFSET_ATTR_NAME = "_offset";
    public static final String TIMESTAMP_ATTR_NAME = "_timestamp";
    // TODO for Protobuf
    public static final String TYPE_ATTR_NAME = "_type";

    private final KGiraffeEngine engine;
    private final List<String> topics;
    private final GraphQLAvroSchemaBuilder avroBuilder;

    public static final GraphQLEnumType orderByEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value(OrderBy.ASC.symbol(), OrderBy.ASC.symbol(), "Ascending")
            .value(OrderBy.DESC.symbol(), OrderBy.DESC.symbol(), "Descending")
            .build();

    public GraphQLSchemaBuilder(KGiraffeEngine engine,
                                List<String> topics) {
        this.engine = engine;
        this.topics = topics;
        this.avroBuilder = new GraphQLAvroSchemaBuilder();
    }

    /**
     * @return A freshly built {@link graphql.schema.GraphQLSchema}
     */
    public GraphQLSchema getGraphQLSchema() {
        GraphQLSchema.Builder schema = GraphQLSchema.newSchema()
            .query(getQueryType())
            .mutation(getMutationType())
            .subscription(getSubscriptionType());
        return schema.build();
    }

    private GraphQLObjectType getQueryType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject()
            .name(QUERY_ROOT)
            .description("Queries for Kafka topics");
        queryType.fields(topics.stream()
            .flatMap(t -> getQueryFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return queryType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        ParsedSchema valueSchema = engine.getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory =
            new GraphQLQueryFactory(engine, topic, keySchema, valueSchema, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(new GraphQLList(objectType))
            .dataFetcher(new EntityFetcher(queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .argument(getLimitArgument())
            .argument(getOffsetArgument())
            .argument(getOrderByArgument(topic, keySchema, valueSchema))
            .build());
    }

    private boolean isObject(ParsedSchema schema) {
        switch (schema.schemaType()) {
            case "AVRO":
                return ((org.apache.avro.Schema) schema.rawSchema()).getType() == Schema.Type.RECORD;
            case "JSON":
                return schema.rawSchema() instanceof ObjectSchema;
            case "PROTOBUF":
            default:
                return true;
        }
    }

    private GraphQLArgument getWhereArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             ParsedSchema valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.QUERY_WHERE, false);
        GraphQLInputObjectType whereInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                ctx, ((AvroSchema) valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(WHERE_PARAM_NAME)
            .description("Where logical specification")
            .type(whereInputObject)
            .build();
    }

    private GraphQLArgument getLimitArgument() {
        return GraphQLArgument.newArgument()
            .name(LIMIT_PARAM_NAME)
            .description("Limit the result set to the given number")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getOffsetArgument() {
        return GraphQLArgument.newArgument()
            .name(OFFSET_PARAM_NAME)
            .description("Start offset for the result set")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getOrderByArgument(String topic,
                                               Either<Type, ParsedSchema> keySchema,
                                               ParsedSchema valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.QUERY_ORDER_BY, false);
        GraphQLInputObjectType orderByInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                ctx, ((AvroSchema) valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();

    }

    private GraphQLObjectType getObjectType(String topic,
                                            Either<Type, ParsedSchema> keySchema,
                                            ParsedSchema valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.OUTPUT, false);
        GraphQLObjectType objectType =
            (GraphQLObjectType) avroBuilder.createOutputType(
                ctx, ((AvroSchema) valueSchema).rawSchema());
        return objectType;
    }

    private GraphQLObjectType getMutationType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder mutationType = GraphQLObjectType.newObject()
            .name(MUTATION_ROOT)
            .description("Mutations for Kafka topics");
        mutationType.fields(topics.stream()
            .flatMap(t -> getMutationFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return mutationType.build();
    }

    private Stream<GraphQLFieldDefinition> getMutationFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        ParsedSchema valueSchema = engine.getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new MutationFetcher(engine, topic, keySchema, valueSchema))
            // TODO
            //.argument(getKeyArgument(topic, keySchema, valueSchema))
            // TODO remove _value
            .argument(getValueArgument(topic, keySchema, valueSchema))
            .build());
    }

    private GraphQLArgument getValueArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             ParsedSchema valueSchema) {
        SchemaContext ctx =
            new SchemaContext(topic, keySchema, valueSchema, Mode.MUTATION, false);
        GraphQLInputObjectType valueInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                ctx, ((AvroSchema) valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(VALUE_ATTR_NAME)
            .description("Value specification")
            .type(valueInputObject)
            .build();
    }

    private GraphQLObjectType getSubscriptionType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder subType = GraphQLObjectType.newObject()
            .name(SUBSCRIPTION_ROOT)
            .description("Subscriptions for Kafka topics");
        subType.fields(topics.stream()
            .flatMap(t -> getQueryFieldStreamDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return subType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldStreamDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        ParsedSchema valueSchema = engine.getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory =
            new GraphQLQueryFactory(engine, topic, keySchema, valueSchema, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new SubscriptionFetcher(engine, topic,
                keySchema, valueSchema, queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .build());
    }
}
