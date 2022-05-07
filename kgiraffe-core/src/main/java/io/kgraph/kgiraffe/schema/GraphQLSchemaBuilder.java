package io.kgraph.kgiraffe.schema;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.schema.PredicateFilter.Criteria;
import io.kgraph.kgiraffe.schema.SchemaContext.Mode;
import io.kgraph.kgiraffe.schema.converters.GraphQLAvroConverter;
import io.kgraph.kgiraffe.schema.converters.GraphQLJsonSchemaConverter;
import io.kgraph.kgiraffe.schema.converters.GraphQLPrimitiveConverter;
import io.kgraph.kgiraffe.schema.converters.GraphQLProtobufConverter;
import io.kgraph.kgiraffe.schema.converters.GraphQLSchemaConverter;
import io.vavr.control.Either;
import org.apache.kafka.common.record.TimestampType;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static graphql.schema.GraphQLNonNull.nonNull;
import static io.kgraph.kgiraffe.KGiraffeEngine.STAGED_SCHEMAS_COLLECTION_NAME;

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

    public static final String HEADERS_ATTR_NAME = "headers";
    public static final String KEY_ATTR_NAME = "key";
    public static final String KEY_ERROR_ATTR_NAME = "key_error";
    public static final String KEY_SCHEMA_ID_ATTR_NAME = "key_schema_id";
    public static final String VALUE_ATTR_NAME = "value";
    public static final String VALUE_ERROR_ATTR_NAME = "value_error";
    public static final String VALUE_SCHEMA_ID_ATTR_NAME = "value_schema_id";
    public static final String TOPIC_ATTR_NAME = "topic";
    public static final String PARTITION_ATTR_NAME = "partition";
    public static final String OFFSET_ATTR_NAME = "offset";
    public static final String TIMESTAMP_ATTR_NAME = "ts";
    public static final String TIMESTAMP_TYPE_ATTR_NAME = "ts_type";
    public static final String LEADER_EPOCH_ATTR_NAME = "leader_epoch";

    public static final String ID_ATTR_NAME = "id";
    public static final String SUBJECT_ATTR_NAME = "subject";
    public static final String VERSION_ATTR_NAME = "version";
    public static final String STATUS_ATTR_NAME = "status";
    public static final String SCHEMA_TYPE_ATTR_NAME = "schema_type";
    public static final String SCHEMA_ATTR_NAME = "schema";
    public static final String SCHEMA_RAW_ATTR_NAME = "schema_raw";
    public static final String REFERENCES_ATTR_NAME = "references";
    public static final String SCHEMA_ERROR_ATTR_NAME = "schema_error";

    public static final String NAME_ATTR_NAME = "name";

    public static final String NEXT_ID_PARAM_NAME = "next_id";
    public static final String PREV_ID_PARAM_NAME = "prev_id";
    public static final String PREV_SUBJECT_PARAM_NAME = "prev_subject";
    public static final String PREV_VERSION_PARAM_NAME = "prev_version";
    public static final String NORMALIZE_PARAM_NAME = "normalize";
    public static final String SUBJECT_PREFIX_PARAM_NAME = "subject_prefix";

    public static final String IS_BACKWARD_COMPATIBLE_ATTR_NAME = "is_backward_compatible";
    public static final String COMPATIBILITY_ERRORS_ATTR_NAME = "compatibility_errors";

    // prefix used for internal field names
    private static final String KAFKA = "kafka";
    private static final String SCHEMA = "schema";

    private final KGiraffeEngine engine;
    private final Set<String> topics;
    private final GraphQLAvroConverter avroConverter;
    private final GraphQLJsonSchemaConverter jsonSchemaConverter;
    private final GraphQLProtobufConverter protobufConverter;
    private final GraphQLPrimitiveConverter primitiveConverter;

    private final Set<String> typeCache = new HashSet<>();

    public static final GraphQLEnumType orderByEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value(OrderBy.ASC.symbol(), OrderBy.ASC.symbol(), "Ascending")
            .value(OrderBy.DESC.symbol(), OrderBy.DESC.symbol(), "Descending")
            .build();

    public static final GraphQLEnumType tsTypeEnum =
        GraphQLEnumType.newEnum()
            .name("ts_type_enum")
            .description("Specifies the timestamp type")
            .value(
                TimestampType.NO_TIMESTAMP_TYPE.toString(),
                TimestampType.NO_TIMESTAMP_TYPE.toString(),
                "Unknown")
            .value(
                TimestampType.CREATE_TIME.toString(),
                TimestampType.CREATE_TIME.toString(),
                "Create")
            .value(
                TimestampType.LOG_APPEND_TIME.toString(),
                TimestampType.LOG_APPEND_TIME.toString(),
                "LogAppend")
            .build();

    public static final GraphQLEnumType statusEnum =
        GraphQLEnumType.newEnum()
            .name("status_enum")
            .description("Schema status")
            .value(Status.STAGED.symbol(), Status.STAGED.symbol(), "Staged")
            .value(Status.REGISTERED.symbol(), Status.REGISTERED.symbol(), "Registered")
            .build();


    public GraphQLSchemaBuilder(KGiraffeEngine engine,
                                Set<String> topics) {
        this.engine = engine;
        this.topics = topics;
        this.avroConverter = new GraphQLAvroConverter();
        this.jsonSchemaConverter = new GraphQLJsonSchemaConverter();
        this.protobufConverter = new GraphQLProtobufConverter();
        this.primitiveConverter = new GraphQLPrimitiveConverter();
    }

    public GraphQLSchemaConverter getSchemaBuilder(Either<Type, ParsedSchema> schema) {
        if (schema.isRight()) {
            ParsedSchema parsedSchema = schema.get();
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    return avroConverter;
                case "JSON":
                    return jsonSchemaConverter;
                case "PROTOBUF":
                    return protobufConverter;
            }
        }
        return primitiveConverter;
    }

    /**
     * @return A freshly built {@link graphql.schema.GraphQLSchema}
     */
    public GraphQLSchema getGraphQLSchema() {
        GraphQLSchema.Builder schema = GraphQLSchema.newSchema()
            .query(getQueryType())
            .mutation(getMutationType());
        if (!topics.isEmpty()) {
            schema.subscription(getSubscriptionType());
        }
        return schema.build();
    }

    private GraphQLObjectType getQueryType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject()
            .name(QUERY_ROOT)
            .description("Queries for Kafka and Schema Registry");
        queryType.field(queryStagedSchemasFieldDefinition(codeRegistry));
        queryType.field(queryRegisteredSchemasFieldDefinition(codeRegistry));
        queryType.field(querySubjectsFieldDefinition(codeRegistry));
        queryType.field(testSchemaCompatibilityFieldDefinition(codeRegistry));
        if (!topics.isEmpty()) {
            queryType.fields(topics.stream()
                .flatMap(t -> getQueryFieldDefinition(codeRegistry, t))
                .collect(Collectors.toList()));
        }

        codeRegistry.build();

        return queryType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLOutputType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory = new GraphQLQueryFactory(engine, topic, objectType);

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

    private GraphQLArgument getWhereArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx = new SchemaContext(topic, Mode.QUERY_WHERE);
        GraphQLInputType keyObject = getSchemaBuilder(keySchema).createInputType(ctx, keySchema);
        if (!(keyObject instanceof GraphQLInputObjectType)) {
            keyObject = createInputFieldOp(ctx, KAFKA, KEY_ATTR_NAME, keyObject);
        }
        GraphQLInputType valueObject = getSchemaBuilder(valueSchema).createInputType(ctx, valueSchema);
        if (!(valueObject instanceof GraphQLInputObjectType)) {
            valueObject = createInputFieldOp(ctx, KAFKA, VALUE_ATTR_NAME, valueObject);
        }

        GraphQLInputType whereInputObject = getWhereObject(ctx, topic, keyObject, valueObject);

        return GraphQLArgument.newArgument()
            .name(WHERE_PARAM_NAME)
            .description("Where logical specification")
            .type(whereInputObject)
            .build();
    }

    private GraphQLInputType getWhereObject(SchemaContext ctx,
                                                  String topic,
                                                  GraphQLInputType keyObject,
                                                  GraphQLInputType valueObject) {
        String name = topic + "_record_criteria";
        if (typeCache.contains(name)) {
            return new GraphQLTypeReference(name);
        } else {
            typeCache.add(name);
        }

        return GraphQLInputObjectType.newInputObject()
            .name(name)
            .description(topic + " record criteria")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(KEY_ATTR_NAME)
                .description("Kafka record key")
                .type(keyObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VALUE_ATTR_NAME)
                .description("Kafka record value")
                .type(valueObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TOPIC_ATTR_NAME)
                .description("Kafka topic")
                .type(createInputFieldOp(ctx, KAFKA, TOPIC_ATTR_NAME, Scalars.GraphQLString))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(PARTITION_ATTR_NAME)
                .description("Kafka partition")
                .type(createInputFieldOp(ctx, KAFKA, PARTITION_ATTR_NAME, Scalars.GraphQLInt))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(OFFSET_ATTR_NAME)
                .description("Kafka record offset")
                .type(createInputFieldOp(ctx, KAFKA, OFFSET_ATTR_NAME, ExtendedScalars.GraphQLLong))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TIMESTAMP_ATTR_NAME)
                .description("Kafka record timestamp")
                .type(createInputFieldOp(ctx, KAFKA, TIMESTAMP_ATTR_NAME, ExtendedScalars.GraphQLLong))
                .build())
            .build();
    }

    public static GraphQLInputType createInputFieldOp(SchemaContext ctx,
                                                      String typeName,
                                                      String fieldName,
                                                      GraphQLInputType fieldType) {
        return createInputFieldOp(ctx.qualify(typeName, fieldName), fieldType);
    }

    public static GraphQLInputType createInputFieldOp(String name,
                                                      GraphQLInputType fieldType) {
        fieldType = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description("Criteria expression specification for " + name)
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.EQ.symbol())
                .description("Equals criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.NEQ.symbol())
                .description("Not equals criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.LTE.symbol())
                .description("Less than or equals criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.GTE.symbol())
                .description("Greater or equals criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.GT.symbol())
                .description("Greater than criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.LT.symbol())
                .description("Less than criteria")
                .type(fieldType)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.IN.symbol())
                .description("In criteria")
                .type(new GraphQLList(fieldType))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(Criteria.NIN.symbol())
                .description("Not in criteria")
                .type(new GraphQLList(fieldType))
                .build())
            .build();
        return fieldType;
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
                                               Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx = new SchemaContext(topic, Mode.QUERY_ORDER_BY);
        GraphQLInputType keyObject = getSchemaBuilder(keySchema).createInputType(ctx, keySchema);
        GraphQLInputType valueObject = getSchemaBuilder(valueSchema).createInputType(ctx, valueSchema);

        String name = topic + "_record_sort";
        GraphQLInputObjectType orderByInputObject = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description(topic + " record sort")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(KEY_ATTR_NAME)
                .description("Kafka record key")
                .type(keyObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VALUE_ATTR_NAME)
                .description("Kafka record value")
                .type(valueObject)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TOPIC_ATTR_NAME)
                .description("Kafka topic")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(PARTITION_ATTR_NAME)
                .description("Kafka partition")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(OFFSET_ATTR_NAME)
                .description("Kafka record offset")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(TIMESTAMP_ATTR_NAME)
                .description("Kafka record timestamp")
                .type(orderByEnum)
                .build())
            .build();

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();

    }

    private GraphQLOutputType getObjectType(String topic,
                                            Either<Type, ParsedSchema> keySchema,
                                            Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx = new SchemaContext(topic, Mode.OUTPUT);

        GraphQLOutputType keyObject = getSchemaBuilder(keySchema).createOutputType(ctx, keySchema);
        GraphQLOutputType valueObject = getSchemaBuilder(valueSchema).createOutputType(ctx, valueSchema);

        String name = topic;
        if (typeCache.contains(name)) {
            return new GraphQLTypeReference(name);
        } else {
            typeCache.add(name);
        }

        return GraphQLObjectType.newObject()
            .name(name)
            .description(topic)
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(HEADERS_ATTR_NAME)
                .description("Kafka record headers")
                .type(ExtendedScalars.Json)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(KEY_ATTR_NAME)
                .description("Kafka record key")
                .type(keyObject)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(KEY_ERROR_ATTR_NAME)
                .description("Kafka record key deserialization error")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(KEY_SCHEMA_ID_ATTR_NAME)
                .description("Kafka record key schema id")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VALUE_ATTR_NAME)
                .description("Kafka record value")
                .type(valueObject)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VALUE_ERROR_ATTR_NAME)
                .description("Kafka record value deserialization error")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VALUE_SCHEMA_ID_ATTR_NAME)
                .description("Kafka record value schema id")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(TOPIC_ATTR_NAME)
                .description("Kafka topic")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(PARTITION_ATTR_NAME)
                .description("Kafka partition")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(OFFSET_ATTR_NAME)
                .description("Kafka record offset")
                .type(ExtendedScalars.GraphQLLong)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(TIMESTAMP_ATTR_NAME)
                .description("Kafka record timestamp")
                .type(ExtendedScalars.GraphQLLong)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(TIMESTAMP_TYPE_ATTR_NAME)
                .description("Kafka record timestamp type")
                .type(tsTypeEnum)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(LEADER_EPOCH_ATTR_NAME)
                .description("Kafka record leader epoch")
                .type(Scalars.GraphQLInt)
                .build())
            .build();
    }

    private GraphQLObjectType getMutationType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder mutationType = GraphQLObjectType.newObject()
            .name(MUTATION_ROOT)
            .description("Mutations for Kafka and Schema Registry");
        mutationType.field(registerSchemaFieldDefinition(codeRegistry));
        mutationType.field(stageSchemaFieldDefinition(codeRegistry));
        mutationType.field(unstageSchemaFieldDefinition(codeRegistry));
        if (!topics.isEmpty()) {
            mutationType.fields(topics.stream()
                .flatMap(t -> getMutationFieldDefinition(codeRegistry, t))
                .collect(Collectors.toList()));
        }

        codeRegistry.build();

        return mutationType.build();
    }

    private Stream<GraphQLFieldDefinition> getMutationFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLOutputType objectType = getObjectType(topic, keySchema, valueSchema);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new MutationFetcher(engine, topic))
            .argument(getHeadersArgument())
            .argument(getKeyArgument(topic, keySchema))
            .argument(getValueArgument(topic, valueSchema))
            .build());
    }

    private GraphQLArgument getHeadersArgument() {
        return GraphQLArgument.newArgument()
            .name(HEADERS_ATTR_NAME)
            .description("Headers specification")
            .type(ExtendedScalars.Json)
            .build();
    }

    private GraphQLArgument getKeyArgument(String topic,
                                           Either<Type, ParsedSchema> keySchema) {
        SchemaContext ctx = new SchemaContext(topic, Mode.MUTATION);
        GraphQLInputType keyObject = getSchemaBuilder(keySchema).createInputType(ctx, keySchema);

        return GraphQLArgument.newArgument()
            .name(KEY_ATTR_NAME)
            .description("Key specification")
            .type(keyObject)
            .build();
    }

    private GraphQLArgument getValueArgument(String topic,
                                             Either<Type, ParsedSchema> valueSchema) {
        SchemaContext ctx = new SchemaContext(topic, Mode.MUTATION);
        GraphQLInputType valueObject = getSchemaBuilder(valueSchema).createInputType(ctx, valueSchema);

        return GraphQLArgument.newArgument()
            .name(VALUE_ATTR_NAME)
            .description("Value specification")
            .type(valueObject)
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
        Either<Type, ParsedSchema> keySchema = engine.getKeySchema(topic);
        Either<Type, ParsedSchema> valueSchema = engine.getValueSchema(topic);

        GraphQLOutputType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory = new GraphQLQueryFactory(engine, topic, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new SubscriptionFetcher(engine, topic, queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .build());
    }

    private GraphQLFieldDefinition queryStagedSchemasFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {

        GraphQLOutputType objectType = getSchemaObjectType();

        GraphQLQueryFactory queryFactory
            = new GraphQLQueryFactory(engine, STAGED_SCHEMAS_COLLECTION_NAME, objectType);

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_query_staged_schemas")
            .type(new GraphQLList(objectType))
            .dataFetcher(new EntityFetcher(queryFactory))
            .argument(getSchemasWhereArgument())
            .argument(getLimitArgument())
            .argument(getOffsetArgument())
            .argument(getSchemasOrderByArgument())
            .build();
    }

    private GraphQLOutputType getSchemaObjectType() {
        String name = "_schema_definition";
        if (typeCache.contains(name)) {
            return new GraphQLTypeReference(name);
        } else {
            typeCache.add(name);
        }
        GraphQLObjectType refType = getSchemaReferenceObjectType();

        return GraphQLObjectType.newObject()
            .name(name)
            .description("Schema definition")
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(ID_ATTR_NAME)
                .description("Schema id")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SUBJECT_ATTR_NAME)
                .description("Schema subject")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VERSION_ATTR_NAME)
                .description("Schema version")
                .type(Scalars.GraphQLInt)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(STATUS_ATTR_NAME)
                .description("Schema status")
                .type(statusEnum)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SCHEMA_TYPE_ATTR_NAME)
                .description("Schema type")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SCHEMA_ATTR_NAME)
                .description("Schema definition")
                .type(ExtendedScalars.Json)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SCHEMA_RAW_ATTR_NAME)
                .description("Schema raw definition")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(REFERENCES_ATTR_NAME)
                .description("Schema references")
                .type(new GraphQLList(refType))
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SCHEMA_ERROR_ATTR_NAME)
                .description("Schema validation error")
                .type(Scalars.GraphQLString)
                .build())
            .build();
    }

    private GraphQLObjectType getSchemaReferenceObjectType() {
        String name = "_schema_reference";

        GraphQLObjectType refType = GraphQLObjectType.newObject()
            .name(name)
            .description("Schema reference")
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(NAME_ATTR_NAME)
                .description("Schema reference name")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(SUBJECT_ATTR_NAME)
                .description("Schema reference subject")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(VERSION_ATTR_NAME)
                .description("Schema reference version")
                .type(Scalars.GraphQLInt)
                .build())
            .build();
        return refType;
    }

    private GraphQLArgument getSchemasWhereArgument() {
        SchemaContext ctx = new SchemaContext(SCHEMA, Mode.QUERY_WHERE);

        GraphQLInputObjectType whereInputObject = getSchemaWhereObject(ctx);

        return GraphQLArgument.newArgument()
            .name(WHERE_PARAM_NAME)
            .description("Where logical specification")
            .type(whereInputObject)
            .build();
    }

    private GraphQLInputObjectType getSchemaWhereObject(SchemaContext ctx) {
        GraphQLInputObjectType type = GraphQLInputObjectType.newInputObject()
            .name("_schema_definition_where_criteria")
            .description("Schema where criteria")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(ID_ATTR_NAME)
                .description("Schema id")
                .type(createInputFieldOp(ctx, SCHEMA, ID_ATTR_NAME, Scalars.GraphQLInt))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(SUBJECT_ATTR_NAME)
                .description("Schema subject")
                .type(createInputFieldOp(ctx, SCHEMA, SUBJECT_ATTR_NAME, Scalars.GraphQLString))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VERSION_ATTR_NAME)
                .description("Schema version")
                .type(createInputFieldOp(ctx, SCHEMA, VERSION_ATTR_NAME, Scalars.GraphQLInt))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(STATUS_ATTR_NAME)
                .description("Schema status")
                .type(createInputFieldOp(ctx, SCHEMA, STATUS_ATTR_NAME, statusEnum))
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(SCHEMA_TYPE_ATTR_NAME)
                .description("Schema type")
                .type(createInputFieldOp(ctx, SCHEMA, SCHEMA_TYPE_ATTR_NAME, Scalars.GraphQLString))
                .build())
            .build();
        return type;
    }

    private GraphQLArgument getSchemasOrderByArgument() {
        GraphQLInputObjectType orderByInputObject = GraphQLInputObjectType.newInputObject()
            .name("_schema_definition_sort")
            .description("Schema sort")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(ID_ATTR_NAME)
                .description("Schema id")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(SUBJECT_ATTR_NAME)
                .description("Schema subject")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VERSION_ATTR_NAME)
                .description("Schema version")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(STATUS_ATTR_NAME)
                .description("Schema status")
                .type(orderByEnum)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(SCHEMA_TYPE_ATTR_NAME)
                .description("Schema type")
                .type(orderByEnum)
                .build())
            .build();

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();
    }

    private GraphQLFieldDefinition testSchemaCompatibilityFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {
        GraphQLObjectType objectType = getCompatibilityObjectType();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_test_schema_compatibility")
            .type(objectType)
            .dataFetcher(new CompatibilityFetcher(engine))
            .argument(getNextSchemaIdArgument())
            .argument(getPrevSchemaIdArgument())
            .argument(getPrevSubjectArgument())
            .argument(getPrevVersionArgument())
            .build();
    }

    private GraphQLObjectType getCompatibilityObjectType() {
        GraphQLObjectType objectType = GraphQLObjectType.newObject()
            .name("_schema_compatibility_result")
            .description("Schema compatibility result")
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(IS_BACKWARD_COMPATIBLE_ATTR_NAME)
                .description("Compatibility result")
                .type(Scalars.GraphQLBoolean)
                .build())
            .field(GraphQLFieldDefinition.newFieldDefinition()
                .name(COMPATIBILITY_ERRORS_ATTR_NAME)
                .description("Compatibility errors")
                .type(new GraphQLList(Scalars.GraphQLString))
                .build())
            .build();

        return objectType;
    }

    private GraphQLArgument getNextSchemaIdArgument() {
        return GraphQLArgument.newArgument()
            .name(NEXT_ID_PARAM_NAME)
            .description("Next schema id")
            .type(nonNull(Scalars.GraphQLInt))
            .build();
    }

    private GraphQLArgument getPrevSchemaIdArgument() {
        return GraphQLArgument.newArgument()
            .name(PREV_ID_PARAM_NAME)
            .description("Previous schema id")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getPrevSubjectArgument() {
        return GraphQLArgument.newArgument()
            .name(PREV_SUBJECT_PARAM_NAME)
            .description("Previous schema subject")
            .type(Scalars.GraphQLString)
            .build();
    }

    private GraphQLArgument getPrevVersionArgument() {
        return GraphQLArgument.newArgument()
            .name(PREV_VERSION_PARAM_NAME)
            .description("Previous schema version")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLFieldDefinition queryRegisteredSchemasFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {
        GraphQLOutputType objectType = getSchemaObjectType();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_query_registered_schemas")
            .type(new GraphQLList(objectType))
            .dataFetcher(new QuerySchemasFetcher(engine))
            .argument(getSchemaIdArgument())
            .argument(getSubjectArgument())
            .argument(getVersionArgument())
            .build();
    }

    private GraphQLArgument getSchemaIdArgument() {
        return GraphQLArgument.newArgument()
            .name(ID_ATTR_NAME)
            .description("Schema id")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLArgument getNonNullSchemaIdArgument() {
        return GraphQLArgument.newArgument()
            .name(ID_ATTR_NAME)
            .description("Schema id")
            .type(nonNull(Scalars.GraphQLInt))
            .build();
    }

    private GraphQLArgument getSubjectArgument() {
        return GraphQLArgument.newArgument()
            .name(SUBJECT_ATTR_NAME)
            .description("Schema subject")
            .type(Scalars.GraphQLString)
            .build();
    }

    private GraphQLArgument getNonNullSubjectArgument() {
        return GraphQLArgument.newArgument()
            .name(SUBJECT_ATTR_NAME)
            .description("Schema subject")
            .type(nonNull(Scalars.GraphQLString))
            .build();
    }

    private GraphQLArgument getVersionArgument() {
        return GraphQLArgument.newArgument()
            .name(VERSION_ATTR_NAME)
            .description("Schema version")
            .type(Scalars.GraphQLInt)
            .build();
    }

    private GraphQLFieldDefinition querySubjectsFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_query_subjects")
            .type(new GraphQLList(Scalars.GraphQLString))
            .dataFetcher(new QuerySubjectsFetcher(engine))
            .argument(getSubjectPrefixArgument())
            .build();
    }

    private GraphQLArgument getSubjectPrefixArgument() {
        return GraphQLArgument.newArgument()
            .name(SUBJECT_PREFIX_PARAM_NAME)
            .description("Schema subject prefix")
            .type(Scalars.GraphQLString)
            .build();
    }

    private GraphQLFieldDefinition registerSchemaFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {
        GraphQLOutputType objectType = getSchemaObjectType();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_register_schema")
            .type(objectType)
            .dataFetcher(new RegistrationFetcher(engine))
            .argument(getNonNullSchemaIdArgument())
            .argument(getNonNullSubjectArgument())
            .argument(getNormalizeArgument())
            .build();
    }

    private GraphQLArgument getNormalizeArgument() {
        return GraphQLArgument.newArgument()
            .name(NORMALIZE_PARAM_NAME)
            .description("Normalization flag")
            .type(Scalars.GraphQLBoolean)
            .build();
    }

    private GraphQLFieldDefinition stageSchemaFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {
        GraphQLOutputType objectType = getSchemaObjectType();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_stage_schema")
            .type(objectType)
            .dataFetcher(new StageFetcher(engine))
            .argument(getSchemaTypeArgument())
            .argument(getSchemaArgument())
            .argument(getReferencesArgument())
            .build();
    }

    private GraphQLArgument getSchemaTypeArgument() {
        return GraphQLArgument.newArgument()
            .name(SCHEMA_TYPE_ATTR_NAME)
            .description("Schema type")
            .type(nonNull(Scalars.GraphQLString))
            .build();
    }

    private GraphQLArgument getSchemaArgument() {
        return GraphQLArgument.newArgument()
            .name(SCHEMA_ATTR_NAME)
            .description("Schema definition")
            .type(nonNull(Scalars.GraphQLString))
            .build();
    }

    private GraphQLArgument getReferencesArgument() {
        return GraphQLArgument.newArgument()
            .name(REFERENCES_ATTR_NAME)
            .description("Schema references")
            .type(new GraphQLList(getSchemaReferenceInputObjectType()))
            .build();
    }

    private GraphQLInputObjectType getSchemaReferenceInputObjectType() {
        String name = "_schema_reference_input";

        GraphQLInputObjectType refType = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description("Schema reference")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(NAME_ATTR_NAME)
                .description("Schema reference name")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(SUBJECT_ATTR_NAME)
                .description("Schema reference subject")
                .type(Scalars.GraphQLString)
                .build())
            .field(GraphQLInputObjectField.newInputObjectField()
                .name(VERSION_ATTR_NAME)
                .description("Schema reference version")
                .type(Scalars.GraphQLInt)
                .build())
            .build();
        return refType;
    }

    private GraphQLFieldDefinition unstageSchemaFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry) {
        GraphQLOutputType objectType = getSchemaObjectType();

        return GraphQLFieldDefinition.newFieldDefinition()
            .name("_unstage_schema")
            .type(objectType)
            .dataFetcher(new UnstageFetcher(engine))
            .argument(getNonNullSchemaIdArgument())
            .build();
    }
}
