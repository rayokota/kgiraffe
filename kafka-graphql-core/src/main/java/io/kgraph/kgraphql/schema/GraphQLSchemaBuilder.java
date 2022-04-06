package io.kgraph.kgraphql.schema;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLImplementingType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.kgraph.kgraphql.schema.SchemaContext.Mode;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.everit.json.schema.ObjectSchema;
import org.ojai.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private static final Logger log = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);

    public static final String QUERY_ROOT = "query_root";

    public static final String LIMIT_PARAM_NAME = "limit";
    public static final String OFFSET_PARAM_NAME = "offset";
    public static final String ORDER_BY_PARAM_NAME = "order_by";
    public static final String WHERE_PARAM_NAME = "where";

    // TODO
    public static final String KEY_ATTR_NAME = "_key";
    // TODO
    public static final String TYPE_ATTR_NAME = "_type";

    private final SchemaRegistryClient schemaRegistry;
    private final List<String> topics;

    public static final GraphQLEnumType orderByEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value("asc", "asc", "Ascending")
            .value("desc", "desc", "Descending")
            .build();

    public GraphQLSchemaBuilder(SchemaRegistryClient schemaRegistry,
                                List<String> topics) {
        this.schemaRegistry = schemaRegistry;
        this.topics = topics;
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
        RuntimeWiring runtimeWiring = buildWiring();
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
            .query(getQueryType());
        return schema.build();
    }

    private GraphQLObjectType getQueryType() {
        // TODO use this instead of deprecated dataFetcher/typeResolver methods
        GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

        GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject()
            .name(QUERY_ROOT)
            .description("GraphQL schema for all Kafka schemas");
        queryType.fields(topics.stream()
            .flatMap(t -> getQueryFieldDefinition(codeRegistry, t))
            .collect(Collectors.toList()));

        codeRegistry.build();

        return queryType.build();
    }

    private Stream<GraphQLFieldDefinition> getQueryFieldDefinition(
        GraphQLCodeRegistry.Builder codeRegistry, String topic) {
        // TODO handle primitive key schemas
        Either<Value.Type, ParsedSchema> keySchema = getKeySchema(topic);
        ParsedSchema valueSchema = getValueSchema(topic);

        if (!isObject(valueSchema)) {
            return Stream.empty();
        }

        GraphQLObjectType objectType = getObjectType(topic, keySchema, valueSchema);

        GraphQLQueryFactory queryFactory = GraphQLQueryFactory.builder()
            .withTopic(topic)
            .withSchemas(keySchema, valueSchema)
            .withImplementingType(objectType)
            .build();

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
                return ((org.apache.avro.Schema)schema.rawSchema()).getType() == Schema.Type.RECORD;
            case "JSON":
                return schema.rawSchema() instanceof ObjectSchema;
            case "PROTOBUF":
            default:
                return true;
        }
    }

    private Either<Value.Type, ParsedSchema> getKeySchema(String topic) {
        Optional<ParsedSchema> keySchema = getLatestSchema(topic + "-key");
        // TODO other primitive keys
        return keySchema.<Either<Value.Type, ParsedSchema>>map(Either::right)
            .orElseGet(() -> Either.left(Value.Type.STRING));
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
                                             Either<Value.Type, ParsedSchema> keySchema,
                                             ParsedSchema valueSchema) {
        GraphQLInputObjectType whereInputObject =
            (GraphQLInputObjectType) new GraphQLAvroSchemaBuilder().createInputType(
                new SchemaContext(topic, Mode.INPUT), ((AvroSchema)valueSchema).rawSchema());

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
                                               Either<Value.Type, ParsedSchema> keySchema,
                                               ParsedSchema valueSchema) {
        String typeName = topic + "_order";

        GraphQLInputObjectType orderByInputObject =
            (GraphQLInputObjectType) new GraphQLAvroSchemaBuilder().createInputType(
                new SchemaContext(topic, Mode.ORDER_BY), ((AvroSchema)valueSchema).rawSchema());

        return GraphQLArgument.newArgument()
            .name(ORDER_BY_PARAM_NAME)
            .description("Order by specification")
            .type(new GraphQLList(orderByInputObject))
            .build();

    }

    private GraphQLObjectType getObjectType(String topic,
                                            Either<Value.Type, ParsedSchema> keySchema,
                                            ParsedSchema valueSchema) {
        GraphQLObjectType objectType =
            (GraphQLObjectType) new GraphQLAvroSchemaBuilder().createOutputType(
                new SchemaContext(topic, Mode.OUTPUT), ((AvroSchema) valueSchema).rawSchema());
        return objectType;
    }
}
