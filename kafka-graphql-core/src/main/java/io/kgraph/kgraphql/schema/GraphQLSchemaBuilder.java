package io.kgraph.kgraphql.schema;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import graphql.Scalars;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.kgraph.kgraphql.schema.SchemaContext.Mode;
import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.everit.json.schema.ObjectSchema;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static graphql.schema.idl.TypeRuntimeWiring.newTypeWiring;

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

    public static final String KEY_PARAM_NAME = "key";
    public static final String VALUE_PARAM_NAME = "value";

    // TODO
    public static final String KEY_ATTR_NAME = "_key";
    // TODO
    public static final String TYPE_ATTR_NAME = "_type";

    private final KafkaGraphQLEngine engine;
    private final SchemaRegistryClient schemaRegistry;
    private final List<String> topics;
    private final GraphQLAvroSchemaBuilder avroBuilder;

    public static final GraphQLEnumType orderByEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value("asc", "asc", "Ascending")
            .value("desc", "desc", "Descending")
            .build();

    public GraphQLSchemaBuilder(KafkaGraphQLEngine engine,
                                SchemaRegistryClient schemaRegistry,
                                List<String> topics) {
        this.engine = engine;
        this.schemaRegistry = schemaRegistry;
        this.topics = topics;
        this.avroBuilder = new GraphQLAvroSchemaBuilder();
    }

    // TODO remove
    /*
    public GraphQLSchema initHello() {
        try {
            URL url = Resources.getResource("schema.graphql");
            String sdl = Resources.toString(url, Charsets.UTF_8);
            return buildSchema(sdl);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphQLSchema buildSchema(String sdl) {
        TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(sdl);
        RuntimeWiring runtimeWiring = b:euildWiring();
        SchemaGenerator schemaGenerator = new SchemaGenerator();
        return schemaGenerator.makeExecutableSchema(typeRegistry, runtimeWiring);
    }

    private RuntimeWiring buildWiring() {
        return RuntimeWiring.newRuntimeWiring()
            .type(newTypeWiring("Query")
                .dataFetcher("hello", getHelloWorldDataFetcher())
                .dataFetcher("echo", getEchoDataFetcher())
                .build())
            .build();

    }
    public DataFetcher getHelloWorldDataFetcher() {
        return environment -> "world";
    }

    public DataFetcher getEchoDataFetcher() {
        return environment -> environment.getArgument("toEcho");
    }

     */


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
        Either<Type, ParsedSchema> keySchema = getKeySchema(topic);
        ParsedSchema valueSchema = getValueSchema(topic);

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

    private Either<Type, ParsedSchema> getKeySchema(String topic) {
        Optional<ParsedSchema> keySchema = getLatestSchema(topic + "-key");
        // TODO other primitive keys
        return keySchema.<Either<Type, ParsedSchema>>map(Either::right)
            .orElseGet(() -> Either.left(Type.STRING));
    }

    private ParsedSchema getValueSchema(String topic) {
        return getLatestSchema(topic + "-value").get();
    }

    private Optional<ParsedSchema> getLatestSchema(String subject) {
        try {
            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
            Optional<ParsedSchema> optSchema =
                schemaRegistry.parseSchema(
                    schemaMetadata.getSchemaType(),
                    schemaMetadata.getSchema(),
                    schemaMetadata.getReferences());
            return optSchema;
        } catch (IOException | RestClientException e) {
            return Optional.empty();
        }
    }

    private GraphQLArgument getWhereArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             ParsedSchema valueSchema) {
        GraphQLInputObjectType whereInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                new SchemaContext(topic, Mode.QUERY_WHERE), ((AvroSchema) valueSchema).rawSchema());

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
        GraphQLInputObjectType orderByInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                new SchemaContext(topic, Mode.QUERY_ORDER_BY), ((AvroSchema) valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();

    }

    private GraphQLObjectType getObjectType(String topic,
                                            Either<Type, ParsedSchema> keySchema,
                                            ParsedSchema valueSchema) {
        GraphQLObjectType objectType =
            (GraphQLObjectType) avroBuilder.createOutputType(
                new SchemaContext(topic, Mode.OUTPUT), ((AvroSchema) valueSchema).rawSchema());
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
        Either<Type, ParsedSchema> keySchema = getKeySchema(topic);
        ParsedSchema valueSchema = getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(objectType)
            .dataFetcher(new MutationFetcher(engine, schemaRegistry, topic, keySchema, valueSchema))
            // TODO
            //.argument(getKeyArgument(topic, keySchema, valueSchema))
            .argument(getValueArgument(topic, keySchema, valueSchema))
            .build());
    }

    private GraphQLArgument getValueArgument(String topic,
                                             Either<Type, ParsedSchema> keySchema,
                                             ParsedSchema valueSchema) {
        GraphQLInputObjectType valueInputObject =
            (GraphQLInputObjectType) avroBuilder.createInputType(
                new SchemaContext(topic, Mode.MUTATION), ((AvroSchema) valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(VALUE_PARAM_NAME)
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
            .flatMap(t -> getSubscriptionFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return subType.build();
    }

    private Stream<GraphQLFieldDefinition> getSubscriptionFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Type, ParsedSchema> keySchema = getKeySchema(topic);
        ParsedSchema valueSchema = getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory =
            new GraphQLQueryFactory(engine, topic, keySchema, valueSchema, objectType);

        return Stream.of(GraphQLFieldDefinition.newFieldDefinition()
            .name(topic)
            .type(new GraphQLList(objectType))
            .dataFetcher(new SubscriptionFetcher(engine, schemaRegistry, topic,
                keySchema, valueSchema, queryFactory))
            .argument(getWhereArgument(topic, keySchema, valueSchema))
            .build());
    }
}
