package io.kgraph.kgraphql.schema;

import graphql.GraphQLContext;
import graphql.GraphQLException;
import graphql.execution.ValuesResolver;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.Field;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.ObjectField;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.language.VariableReference;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLImplementingType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.InputValueWithState;
import graphql.schema.SelectedField;
import io.hdocdb.store.HDocumentDB;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.kgraph.kgraphql.schema.util.DataFetchingEnvironmentBuilder;
import io.vavr.control.Either;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.Value.Type;
import org.ojai.store.DocumentStore;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static graphql.introspection.Introspection.SchemaMetaFieldDef;
import static graphql.introspection.Introspection.TypeMetaFieldDef;
import static graphql.introspection.Introspection.TypeNameMetaFieldDef;
import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.LIMIT_PARAM_NAME;
import static io.kgraph.kgraphql.schema.GraphQLSchemaBuilder.OFFSET_PARAM_NAME;

public class GraphQLQueryFactory {

    private final static Logger LOG = LoggerFactory.getLogger(GraphQLQueryFactory.class);

    public static final String DESC = "desc";

    private final HDocumentDB docdb;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;
    private final GraphQLObjectType objectType;

    public GraphQLQueryFactory(HDocumentDB docdb,
                               String topic,
                               Either<Type, ParsedSchema> keySchema,
                               ParsedSchema valueSchema,
                               GraphQLObjectType objectType) {
        this.docdb = docdb;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.objectType = objectType;
    }

    public DocumentStream queryResult(DataFetchingEnvironment env) {
        GraphQLContext context = env.getGraphQlContext();

        QueryCondition query = getCriteriaQuery(env, env.getField());

        String orderByPath = getOrderByPath(env, env.getField());

        int limit = 0;
        Optional<Argument> limitArg = getArgument(env.getField(), LIMIT_PARAM_NAME);
        if (limitArg.isPresent()) {
            IntValue limitValue = getValue(limitArg.get(), env);
            limit = limitValue.getValue().intValue();
        }

        int offset = 0;
        Optional<Argument> offsetArg = getArgument(env.getField(), OFFSET_PARAM_NAME);
        if (offsetArg.isPresent()) {
            IntValue offsetValue = getValue(offsetArg.get(), env);
            offset = offsetValue.getValue().intValue();
        }

        DocumentStore coll = docdb.getCollection(topic);
        DocumentStream result = query == null || query.isEmpty() ? coll.find() : coll.find(query);
        return result;
    }

    protected QueryCondition getCriteriaQuery(DataFetchingEnvironment environment, Field field) {
        return null;

        /*
        // Build predicates from query arguments
        List<FilterCriteria> predicates = getFieldPredicates(field, environment);

        return getCompoundPredicate(predicates, Logical.AND);

         */
    }

    protected String getOrderByPath(DataFetchingEnvironment environment, Field field) {
        return null;
    }

    /*
    protected List<FilterCriteria> getFieldPredicates(Field field,
                                                      DataFetchingEnvironment environment) {

        List<FilterCriteria> predicates = new ArrayList<>();

        field.getArguments().stream()
            .map(it -> getPredicate(field, environment, it))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return predicates;
    }

     */

    protected Optional<Argument> getArgument(Field selectedField, String argumentName) {
        return selectedField.getArguments().stream()
            .filter(it -> it.getName().equals(argumentName))
            .findFirst();
    }

    /*
    protected FilterCriteria getPredicate(Field field, DataFetchingEnvironment environment,
                                          Argument argument) {
        if (!GraphQLSupport.isWhereArgument(argument)) {
            return null;
        }

        return getWherePredicate(argumentEnvironment(environment, argument), argument);
    }

     */

    @SuppressWarnings("unchecked")
    private <R extends Value<?>> R getValue(Argument argument, DataFetchingEnvironment environment) {
        Value<?> value = argument.getValue();

        if (value instanceof VariableReference) {
            Object variableValue = getVariableReferenceValue((VariableReference) value, environment);

            GraphQLArgument graphQLArgument = environment.getExecutionStepInfo()
                .getFieldDefinition()
                .getArgument(argument.getName());

            return (R) ValuesResolver.valueToLiteral(
                InputValueWithState.newExternalValue(variableValue), graphQLArgument.getType());
        }

        return (R) value;
    }

    private Object getVariableReferenceValue(VariableReference variableReference,
                                             DataFetchingEnvironment env) {
        return env.getVariables().get(variableReference.getName());
    }

    /*
    protected FilterCriteria getWherePredicate(DataFetchingEnvironment environment,
                                               Argument argument) {
        ObjectValue whereValue = getValue(argument, environment);

        if (whereValue.getChildren().isEmpty()) {
            return new FilterCriteria();
        }

        Logical logical = extractLogical(argument);

        Map<String, Object> predicateArguments = new LinkedHashMap<>();
        predicateArguments.put(logical.symbol(), environment.getArguments());

        DataFetchingEnvironment predicateDataFetchingEnvironment = DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(
                environment)
            .arguments(predicateArguments)
            .build();
        Argument predicateArgument = new Argument(logical.symbol(), whereValue);

        return getArgumentPredicate(predicateDataFetchingEnvironment, predicateArgument);
    }

    protected FilterCriteria getArgumentPredicate(
        DataFetchingEnvironment environment, Argument argument) {
        ObjectValue whereValue = getValue(argument, environment);

        if (whereValue.getChildren().isEmpty()) {
            return new FilterCriteria();
        }

        Logical logical = extractLogical(argument);

        List<FilterCriteria> predicates = new ArrayList<>();

        whereValue.getObjectFields().stream()
            .filter(it -> Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> arguments = getFieldArguments(environment, it, argument);

                if (it.getValue() instanceof ArrayValue) {
                    return getArgumentsPredicate(argumentEnvironment(environment, arguments),
                        new Argument(it.getName(), it.getValue()));
                }

                return getArgumentPredicate(argumentEnvironment(environment, arguments),
                    new Argument(it.getName(), it.getValue()));
            })
            .forEach(predicates::add);

        whereValue.getObjectFields().stream()
            .filter(it -> !Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> args = getFieldArguments(environment, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                return getObjectFieldPredicate(environment, logical, it, arg, args);
            })
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }


    protected FilterCriteria getObjectFieldPredicate(DataFetchingEnvironment environment,
                                                     Logical logical,
                                                     ObjectField objectField,
                                                     Argument argument,
                                                     Map<String, Object> arguments
    ) {
        return getLogicalPredicate(objectField.getName(),
            objectField,
            argumentEnvironment(environment, arguments),
            argument);
    }

    protected FilterCriteria getArgumentsPredicate(DataFetchingEnvironment environment,
                                                   Argument argument) {
        ArrayValue whereValue = getValue(argument, environment);

        if (whereValue.getValues().isEmpty()) {
            return new FilterCriteria();
        }

        Logical logical = extractLogical(argument);

        List<FilterCriteria> predicates = new ArrayList<>();

        List<Map<String, Object>> arguments = environment.getArgument(logical.symbol());
        List<ObjectValue> values = whereValue.getValues()
            .stream()
            .map(ObjectValue.class::cast).collect(Collectors.toList());

        List<SimpleEntry<ObjectValue, Map<String, Object>>> tuples =
            IntStream.range(0, values.size())
                .mapToObj(i -> new SimpleEntry<>(values.get(i),
                    arguments.get(i)))
                .collect(Collectors.toList());

        tuples.stream()
            .flatMap(e -> e.getKey()
                .getObjectFields()
                .stream()
                .filter(it -> Logical.symbols().contains(it.getName()))
                .map(it -> {
                    Map<String, Object> args = e.getValue();
                    Argument arg = new Argument(it.getName(), it.getValue());

                    if (it.getValue() instanceof ArrayValue) {
                        return getArgumentsPredicate(argumentEnvironment(environment, args),
                            arg);
                    }

                    return getArgumentPredicate(argumentEnvironment(environment, args),
                        arg);

                }))
            .forEach(predicates::add);

        tuples.stream()
            .flatMap(e -> e.getKey()
                .getObjectFields()
                .stream()
                .filter(it -> !Logical.symbols().contains(it.getName()))
                .map(it -> {
                    Map<String, Object> args = e.getValue();
                    Argument arg = new Argument(it.getName(), it.getValue());

                    return getObjectFieldPredicate(environment, logical, it, arg, args);
                }))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }

     */

    private Map<String, Object> getFieldArguments(DataFetchingEnvironment environment,
                                                  ObjectField field, Argument argument) {
        Map<String, Object> arguments;

        if (environment.getArgument(argument.getName()) instanceof Collection) {
            Collection<Map<String, Object>> list = environment.getArgument(argument.getName());

            arguments = list.stream()
                .filter(args -> args.get(field.getName()) != null)
                .findFirst()
                .orElse(list.stream().findFirst().get());
        } else {
            arguments = environment.getArgument(argument.getName());
        }

        return arguments;
    }

    private Logical extractLogical(Argument argument) {
        return Optional.of(argument.getName())
            .filter(it -> Logical.symbols().contains(it))
            .map(Logical::get)
            .orElse(Logical.AND);
    }

    /*
    private FilterCriteria getLogicalPredicates(String fieldName,
                                                ObjectField objectField,
                                                DataFetchingEnvironment environment,
                                                Argument argument) {
        ArrayValue value = (ArrayValue) objectField.getValue();

        Logical logical = extractLogical(argument);

        List<FilterCriteria> predicates = new ArrayList<>();

        value.getValues()
            .stream()
            .map(ObjectValue.class::cast)
            .flatMap(it -> it.getObjectFields().stream())
            .map(it -> {
                Map<String, Object> args = getFieldArguments(environment, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                return getLogicalPredicate(it.getName(),
                    it,
                    argumentEnvironment(environment, args),
                    arg);
            })
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }

    private FilterCriteria getLogicalPredicate(String fieldName, ObjectField objectField,
                                               DataFetchingEnvironment environment, Argument argument) {
        ObjectValue expressionValue;

        if (objectField.getValue() instanceof ObjectValue) {
            expressionValue = (ObjectValue) objectField.getValue();
        } else {
            expressionValue = new ObjectValue(Collections.singletonList(objectField));
        }

        if (expressionValue.getChildren().isEmpty()) {
            return new FilterCriteria();
        }

        Logical logical = extractLogical(argument);

        List<FilterCriteria> predicates = new ArrayList<>();

        // Let's parse logical expressions, i.e. AND, OR
        expressionValue.getObjectFields().stream()
            .filter(it -> Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> args = getFieldArguments(environment, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                if (it.getValue() instanceof ArrayValue) {
                    return getLogicalPredicates(fieldName, it,
                        argumentEnvironment(environment, args),
                        arg);
                }

                return getLogicalPredicate(fieldName, it,
                    argumentEnvironment(environment, args),
                    arg);
            })
            .forEach(predicates::add);

        // Let's parse relation criteria expressions if present, i.e. books, author, etc.
        if (expressionValue.getObjectFields()
            .stream()
            .anyMatch(it -> !Logical.symbols().contains(it.getName())
                && !Criteria.symbols().contains(it.getName()))) {
            GraphQLFieldDefinition fieldDefinition = getFieldDefinition(environment.getGraphQLSchema(),
                this.getImplementingType(environment),
                new Field(fieldName));
            Map<String, Object> args = new LinkedHashMap<>();
            Argument arg = new Argument(logical.symbol(), expressionValue);
            boolean isOptional = false;

            if (Logical.symbols().contains(argument.getName())) {
                args.put(logical.symbol(), environment.getArgument(argument.getName()));
            } else {
                args.put(logical.symbol(), environment.getArgument(fieldName));
            }

            return getArgumentPredicate(wherePredicateEnvironment(environment, fieldDefinition, args),
                arg);
        }

        // Let's parse simple Criteria expressions, i.e. EQ, LIKE, etc.
        expressionValue.getObjectFields()
            .stream()
            .filter(it -> Criteria.symbols().contains(it.getName()))
            .map(it -> getPredicateFilter(new ObjectField(fieldName, it.getValue()),
                argumentEnvironment(environment, argument),
                new Argument(it.getName(), it.getValue())))
            .sorted()
            .map(it -> it.toFilterCriteria(environment))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);

    }

    private FilterCriteria getCompoundPredicate(List<FilterCriteria> predicates, Logical logical) {
        if (predicates.isEmpty()) {
            return new FilterCriteria();
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        FilterCriteria criteria = new FilterCriteria();
        switch (logical) {
            case OR:
                criteria.setCondition(Condition.OR);
                break;
            case AND:
                criteria.setCondition(Condition.AND);
                break;
            default:
                throw new IllegalArgumentException();
        }
        criteria.setCriterion(predicates);
        return criteria;
    }

    private PredicateFilter getPredicateFilter(ObjectField objectField,
                                               DataFetchingEnvironment environment, Argument argument) {
        PredicateFilter.Criteria option = PredicateFilter.Criteria.get(argument.getName());

        Map<String, Object> valueArguments = new LinkedHashMap<>();
        valueArguments.put(objectField.getName(), environment.getArgument(argument.getName()));

        DataFetchingEnvironment dataFetchingEnvironment = DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(
                environment)
            .arguments(valueArguments)
            .build();

        Argument dataFetchingArgument = new Argument(objectField.getName(), argument.getValue());

        Object filterValue = convertValue(dataFetchingEnvironment, dataFetchingArgument,
            argument.getValue());

        return new PredicateFilter(objectField.getName(), filterValue, option);
    }

     */

    protected DataFetchingEnvironment argumentEnvironment(DataFetchingEnvironment environment,
                                                          Map<String, Object> arguments) {
        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(environment)
            .arguments(arguments)
            .build();
    }

    protected DataFetchingEnvironment argumentEnvironment(DataFetchingEnvironment environment,
                                                          Argument argument) {
        Map<String, Object> arguments = environment.getArgument(argument.getName());

        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(environment)
            .arguments(arguments)
            .build();
    }

    protected DataFetchingEnvironment wherePredicateEnvironment(DataFetchingEnvironment environment,
                                                                GraphQLFieldDefinition fieldDefinition, Map<String, Object> arguments) {
        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(environment)
            .arguments(arguments)
            .fieldDefinition(fieldDefinition)
            .fieldType(fieldDefinition.getType())
            .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Object convertValue(DataFetchingEnvironment environment, Argument argument,
                                  Value value) {
        if (value instanceof NullValue) {
            return value;
        } else if (value instanceof StringValue) {
            Object convertedValue = environment.getArgument(argument.getName());
            if (convertedValue != null) {
                // Return real typed resolved value even if the Value is a StringValue
                return convertedValue;
            } else {
                // Return provided StringValue
                return ((StringValue) value).getValue();
            }
        } else if (value instanceof VariableReference) {
            // Get resolved variable in environment arguments
            return environment.getVariables().get(((VariableReference) value).getName());
        } else if (value instanceof ArrayValue) {
            Collection arrayValue = environment.getArgument(argument.getName());

            if (arrayValue != null) {
                // Let's unwrap array of array values
                if (arrayValue.stream()
                    .allMatch(it -> it instanceof Collection)) {
                    return arrayValue.iterator().next();
                }

                // Let's try handle Ast Value types
                else if (arrayValue.stream()
                    .anyMatch(it -> it instanceof Value)) {
                    return arrayValue.stream()
                        .map(it -> convertValue(environment,
                            argument,
                            (Value) it))
                        .collect(Collectors.toList());
                }
                // Return real typed resolved array value, i.e. Date, UUID, Long
                else {
                    return arrayValue;
                }
            } else {
                // Wrap converted values in ArrayList
                return ((ArrayValue) value).getValues().stream()
                    .map((it) -> convertValue(environment, argument, it))
                    .collect(Collectors.toList());
            }

        } else if (value instanceof EnumValue) {
            return ((EnumValue) value).getName();
        } else if (value instanceof IntValue) {
            return ((IntValue) value).getValue();
        } else if (value instanceof BooleanValue) {
            return ((BooleanValue) value).isValue();
        } else if (value instanceof FloatValue) {
            return ((FloatValue) value).getValue();
        } else if (value instanceof ObjectValue) {
            Map<String, Object> values = environment.getArgument(argument.getName());
            return values;
        }

        return value;
    }

    /**
     * Resolve GraphQL object type from Argument output type.
     *
     * @param environment the environment
     * @return resolved GraphQL object type or null if no output type is provided
     */
    private GraphQLImplementingType getImplementingType(DataFetchingEnvironment environment) {
        GraphQLType outputType = environment.getFieldType();

        if (outputType instanceof GraphQLList) {
            outputType = ((GraphQLList) outputType).getWrappedType();
        }

        if (outputType instanceof GraphQLImplementingType) {
            return (GraphQLImplementingType) outputType;
        }

        return null;
    }

    protected GraphQLFieldDefinition getFieldDefinition(GraphQLSchema schema,
                                                        GraphQLFieldsContainer parentType, Field field) {
        if (schema.getQueryType() == parentType) {
            if (field.getName().equals(SchemaMetaFieldDef.getName())) {
                return SchemaMetaFieldDef;
            }
            if (field.getName().equals(TypeMetaFieldDef.getName())) {
                return TypeMetaFieldDef;
            }
        }
        if (field.getName().equals(TypeNameMetaFieldDef.getName())) {
            return TypeNameMetaFieldDef;
        }

        GraphQLFieldDefinition fieldDefinition = parentType.getFieldDefinition(field.getName());

        if (fieldDefinition != null) {
            return fieldDefinition;
        }

        throw new GraphQLException("unknown field " + field.getName());
    }

    public GraphQLObjectType getObjectType() {
        return objectType;
    }
}
