package io.kgraph.kgiraffe.schema;

import com.google.common.collect.Iterables;
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
import io.hdocdb.HValue;
import io.hdocdb.store.HQueryCondition;
import io.kcache.utils.Streams;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.schema.PredicateFilter.Criteria;
import io.kgraph.kgiraffe.schema.util.DataFetchingEnvironmentBuilder;
import io.kgraph.kgiraffe.schema.util.GraphQLSupport;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import org.ojai.Document;
import org.ojai.Value.Type;
import org.ojai.store.DocumentStore;
import org.ojai.store.QueryCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;

import static graphql.introspection.Introspection.SchemaMetaFieldDef;
import static graphql.introspection.Introspection.TypeMetaFieldDef;
import static graphql.introspection.Introspection.TypeNameMetaFieldDef;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.LIMIT_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.OFFSET_PARAM_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.ORDER_BY_PARAM_NAME;

public class GraphQLQueryFactory {

    private final static Logger LOG = LoggerFactory.getLogger(GraphQLQueryFactory.class);

    private final KGiraffeEngine engine;
    private final String topic;
    private final Either<Type, ParsedSchema> keySchema;
    private final ParsedSchema valueSchema;
    private final GraphQLObjectType objectType;

    public GraphQLQueryFactory(KGiraffeEngine engine,
                               String topic,
                               Either<Type, ParsedSchema> keySchema,
                               ParsedSchema valueSchema,
                               GraphQLObjectType objectType) {
        this.engine = engine;
        this.topic = topic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.objectType = objectType;
    }

    public Iterable<Document> queryResult(DataFetchingEnvironment env) {
        GraphQLContext context = env.getGraphQlContext();
        QueryCondition query = getCriteriaQuery(env, env.getField());
        DocumentStore coll = engine.getDocDB().getCollection(topic);
        Iterable<Document> result = query == null || query.isEmpty() ? coll.find() : coll.find(query);

        Optional<Argument> offsetArg = getArgument(env.getField(), OFFSET_PARAM_NAME);
        if (offsetArg.isPresent()) {
            IntValue offsetValue = getValue(offsetArg.get(), env);
            int offset = offsetValue.getValue().intValue();
            result = Iterables.skip(result, offset);
        }

        Optional<Argument> limitArg = getArgument(env.getField(), LIMIT_PARAM_NAME);
        if (limitArg.isPresent()) {
            IntValue limitValue = getValue(limitArg.get(), env);
            int limit = limitValue.getValue().intValue();
            result = Iterables.limit(result, limit);
        }

        Tuple2<String, OrderBy> orderBy = getOrderBy(env, env.getField());
        if (orderBy != null) {
            Comparator<Document> cmp = orderBy._2 == OrderBy.ASC
                ? Comparator.comparing(doc -> (HValue) doc.getValue(orderBy._1),
                Comparator.nullsFirst(Comparator.naturalOrder()))
                : Comparator.comparing(doc -> (HValue) doc.getValue(orderBy._1),
                Comparator.nullsFirst(Comparator.reverseOrder()));
            Stream<Document> stream = Streams.streamOf(result).sorted(cmp);
            result = stream::iterator;
        }

        return result;
    }

    public HQueryCondition getCriteriaQuery(DataFetchingEnvironment env, Field field) {
        // Build predicates from query arguments
        List<HQueryCondition> predicates = getFieldPredicates(field, env);

        return getCompoundPredicate(predicates, Logical.AND);
    }

    protected Tuple2<String, OrderBy> getOrderBy(DataFetchingEnvironment env, Field field) {
        String orderByPath = null;
        OrderBy orderByDirection = null;
        Optional<Argument> orderByArg = getArgument(field, ORDER_BY_PARAM_NAME);
        if (orderByArg.isPresent()) {
            ObjectValue objectValue = getValue(orderByArg.get(), env);
            while (objectValue != null) {
                if (objectValue.getObjectFields().size() > 0) {
                    // Only use the first attr set for this object
                    ObjectField objectField = objectValue.getObjectFields().get(0);
                    if (orderByPath == null) {
                        orderByPath = objectField.getName();
                    } else {
                        orderByPath += "." + objectField.getName();
                    }
                    Value value = objectField.getValue();
                    if (value instanceof EnumValue) {
                        orderByDirection = OrderBy.get(((EnumValue) value).getName());
                        break;
                    } else {
                        objectValue = (ObjectValue) value;
                    }
                }
            }
        }
        return orderByPath != null && orderByDirection != null
            ? new Tuple2<>(orderByPath, orderByDirection)
            : null;
    }

    protected List<HQueryCondition> getFieldPredicates(Field field,
                                                       DataFetchingEnvironment env) {

        List<HQueryCondition> predicates = new ArrayList<>();

        field.getArguments().stream()
            .map(it -> getPredicate(field, env, it))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return predicates;
    }

    protected Optional<Argument> getArgument(Field selectedField, String argumentName) {
        return selectedField.getArguments().stream()
            .filter(it -> it.getName().equals(argumentName))
            .findFirst();
    }

    protected HQueryCondition getPredicate(Field field, DataFetchingEnvironment env,
                                           Argument argument) {
        if (!GraphQLSupport.isWhereArgument(argument)) {
            return null;
        }

        return getWherePredicate(argumentEnvironment(env, argument), argument);
    }

    @SuppressWarnings("unchecked")
    private <R extends Value<?>> R getValue(Argument argument, DataFetchingEnvironment env) {
        Value<?> value = argument.getValue();

        if (value instanceof VariableReference) {
            Object variableValue = getVariableReferenceValue((VariableReference) value, env);

            GraphQLArgument graphQLArgument = env.getExecutionStepInfo()
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

    protected HQueryCondition getWherePredicate(DataFetchingEnvironment env,
                                                Argument argument) {
        ObjectValue whereValue = getValue(argument, env);

        if (whereValue.getChildren().isEmpty()) {
            return new HQueryCondition();
        }

        Logical logical = extractLogical(argument);

        Map<String, Object> predicateArguments = new LinkedHashMap<>();
        predicateArguments.put(logical.symbol(), env.getArguments());

        DataFetchingEnvironment predicateDataFetchingEnvironment = DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(
                env)
            .arguments(predicateArguments)
            .build();
        Argument predicateArgument = new Argument(logical.symbol(), whereValue);

        return getArgumentPredicate(predicateDataFetchingEnvironment, predicateArgument);
    }

    protected HQueryCondition getArgumentPredicate(
        DataFetchingEnvironment env, Argument argument) {
        ObjectValue whereValue = getValue(argument, env);

        if (whereValue.getChildren().isEmpty()) {
            return new HQueryCondition();
        }

        Logical logical = extractLogical(argument);

        List<HQueryCondition> predicates = new ArrayList<>();

        whereValue.getObjectFields().stream()
            .filter(it -> Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> arguments = getFieldArguments(env, it, argument);

                if (it.getValue() instanceof ArrayValue) {
                    return getArgumentsPredicate(argumentEnvironment(env, arguments),
                        new Argument(it.getName(), it.getValue()));
                }

                return getArgumentPredicate(argumentEnvironment(env, arguments),
                    new Argument(it.getName(), it.getValue()));
            })
            .forEach(predicates::add);

        whereValue.getObjectFields().stream()
            .filter(it -> !Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> args = getFieldArguments(env, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                return getObjectFieldPredicate(env, logical, it, arg, args);
            })
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }


    protected HQueryCondition getObjectFieldPredicate(DataFetchingEnvironment env,
                                                      Logical logical,
                                                      ObjectField objectField,
                                                      Argument argument,
                                                      Map<String, Object> arguments
    ) {
        return getLogicalPredicate(objectField.getName(),
            objectField,
            argumentEnvironment(env, arguments),
            argument);
    }

    protected HQueryCondition getArgumentsPredicate(DataFetchingEnvironment env,
                                                    Argument argument) {
        ArrayValue whereValue = getValue(argument, env);

        if (whereValue.getValues().isEmpty()) {
            return new HQueryCondition();
        }

        Logical logical = extractLogical(argument);

        List<HQueryCondition> predicates = new ArrayList<>();

        List<Map<String, Object>> arguments = env.getArgument(logical.symbol());
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
                        return getArgumentsPredicate(argumentEnvironment(env, args),
                            arg);
                    }

                    return getArgumentPredicate(argumentEnvironment(env, args),
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

                    return getObjectFieldPredicate(env, logical, it, arg, args);
                }))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }

    private Map<String, Object> getFieldArguments(DataFetchingEnvironment env,
                                                  ObjectField field, Argument argument) {
        Map<String, Object> arguments;

        if (env.getArgument(argument.getName()) instanceof Collection) {
            Collection<Map<String, Object>> list = env.getArgument(argument.getName());

            arguments = list.stream()
                .filter(args -> args.get(field.getName()) != null)
                .findFirst()
                .orElse(list.stream().findFirst().get());
        } else {
            arguments = env.getArgument(argument.getName());
        }

        return arguments;
    }

    private Logical extractLogical(Argument argument) {
        return Optional.of(argument.getName())
            .filter(it -> Logical.symbols().contains(it))
            .map(Logical::get)
            .orElse(Logical.AND);
    }

    private HQueryCondition getLogicalPredicates(String fieldName,
                                                 ObjectField objectField,
                                                 DataFetchingEnvironment env,
                                                 Argument argument) {
        ArrayValue value = (ArrayValue) objectField.getValue();

        Logical logical = extractLogical(argument);

        List<HQueryCondition> predicates = new ArrayList<>();

        value.getValues()
            .stream()
            .map(ObjectValue.class::cast)
            .flatMap(it -> it.getObjectFields().stream())
            .map(it -> {
                Map<String, Object> args = getFieldArguments(env, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                return getLogicalPredicate(it.getName(),
                    it,
                    argumentEnvironment(env, args),
                    arg);
            })
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);
    }

    private HQueryCondition getLogicalPredicate(String fieldName,
                                                ObjectField objectField,
                                                DataFetchingEnvironment env,
                                                Argument argument) {
        ObjectValue expressionValue;

        if (objectField.getValue() instanceof ObjectValue) {
            expressionValue = (ObjectValue) objectField.getValue();
        } else {
            expressionValue = new ObjectValue(Collections.singletonList(objectField));
        }

        if (expressionValue.getChildren().isEmpty()) {
            return new HQueryCondition();
        }

        Logical logical = extractLogical(argument);

        List<HQueryCondition> predicates = new ArrayList<>();

        // Let's parse logical expressions, i.e. AND, OR
        expressionValue.getObjectFields().stream()
            .filter(it -> Logical.symbols().contains(it.getName()))
            .map(it -> {
                Map<String, Object> args = getFieldArguments(env, it, argument);
                Argument arg = new Argument(it.getName(), it.getValue());

                if (it.getValue() instanceof ArrayValue) {
                    return getLogicalPredicates(fieldName, it,
                        argumentEnvironment(env, args),
                        arg);
                }

                return getLogicalPredicate(fieldName, it,
                    argumentEnvironment(env, args),
                    arg);
            })
            .forEach(predicates::add);

        // Let's parse relation criteria expressions if present, i.e. books, author, etc.
        if (expressionValue.getObjectFields()
            .stream()
            .anyMatch(it -> !Logical.symbols().contains(it.getName())
                && !Criteria.symbols().contains(it.getName()))) {
            GraphQLFieldDefinition fieldDefinition = getFieldDefinition(env.getGraphQLSchema(),
                this.getImplementingType(env),
                new Field(fieldName));
            Map<String, Object> args = new LinkedHashMap<>();
            Argument arg = new Argument(logical.symbol(), expressionValue);

            if (Logical.symbols().contains(argument.getName())) {
                args.put(logical.symbol(), env.getArgument(argument.getName()));
            } else {
                args.put(logical.symbol(), env.getArgument(fieldName));
            }

            return getArgumentPredicate(wherePredicateEnvironment(env, fieldDefinition, args),
                arg);
        }

        // Let's parse simple Criteria expressions, i.e. EQ, LIKE, etc.
        expressionValue.getObjectFields()
            .stream()
            .filter(it -> Criteria.symbols().contains(it.getName()))
            .map(it -> getPredicateFilter(new ObjectField(fieldName, it.getValue()),
                argumentEnvironment(env, argument),
                new Argument(it.getName(), it.getValue())))
            .sorted()
            .map(it -> it.toQueryCondition(env))
            .filter(Objects::nonNull)
            .forEach(predicates::add);

        return getCompoundPredicate(predicates, logical);

    }

    private HQueryCondition getCompoundPredicate(List<HQueryCondition> predicates, Logical logical) {
        if (predicates.isEmpty()) {
            return new HQueryCondition();
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        HQueryCondition criteria = new HQueryCondition();
        switch (logical) {
            case OR:
                criteria.or();
                break;
            case AND:
                criteria.and();
                break;
            default:
                throw new IllegalArgumentException();
        }
        for (HQueryCondition predicate : predicates) {
            if (!predicate.isEmpty()) {
                criteria.condition(predicate);
            }
        }
        criteria.close();
        return criteria;
    }

    private PredicateFilter getPredicateFilter(ObjectField objectField,
                                               DataFetchingEnvironment env, Argument argument) {
        PredicateFilter.Criteria option = PredicateFilter.Criteria.get(argument.getName());

        Map<String, Object> valueArguments = new LinkedHashMap<>();
        valueArguments.put(objectField.getName(), env.getArgument(argument.getName()));

        DataFetchingEnvironment dataFetchingEnvironment = DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(
                env)
            .arguments(valueArguments)
            .build();

        Argument dataFetchingArgument = new Argument(objectField.getName(), argument.getValue());

        Object filterValue = convertValue(dataFetchingEnvironment, dataFetchingArgument,
            argument.getValue());

        return new PredicateFilter(objectField.getName(), filterValue, option);
    }

    protected DataFetchingEnvironment argumentEnvironment(DataFetchingEnvironment env,
                                                          Map<String, Object> arguments) {
        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(env)
            .arguments(arguments)
            .build();
    }

    protected DataFetchingEnvironment argumentEnvironment(DataFetchingEnvironment env,
                                                          Argument argument) {
        Map<String, Object> arguments = env.getArgument(argument.getName());

        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(env)
            .arguments(arguments)
            .build();
    }

    protected DataFetchingEnvironment wherePredicateEnvironment(DataFetchingEnvironment env,
                                                                GraphQLFieldDefinition fieldDefinition, Map<String, Object> arguments) {
        return DataFetchingEnvironmentBuilder.newDataFetchingEnvironment(env)
            .arguments(arguments)
            .fieldDefinition(fieldDefinition)
            .fieldType(fieldDefinition.getType())
            .build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Object convertValue(DataFetchingEnvironment env, Argument argument,
                                  Value value) {
        if (value instanceof NullValue) {
            return value;
        } else if (value instanceof StringValue) {
            Object convertedValue = env.getArgument(argument.getName());
            if (convertedValue != null) {
                // Return real typed resolved value even if the Value is a StringValue
                return convertedValue;
            } else {
                // Return provided StringValue
                return ((StringValue) value).getValue();
            }
        } else if (value instanceof VariableReference) {
            // Get resolved variable in environment arguments
            return env.getVariables().get(((VariableReference) value).getName());
        } else if (value instanceof ArrayValue) {
            Collection arrayValue = env.getArgument(argument.getName());

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
                        .map(it -> convertValue(env,
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
                    .map((it) -> convertValue(env, argument, it))
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
            Map<String, Object> values = env.getArgument(argument.getName());
            return values;
        }

        return value;
    }

    /**
     * Resolve GraphQL object type from Argument output type.
     *
     * @param env the environment
     * @return resolved GraphQL object type or null if no output type is provided
     */
    private GraphQLImplementingType getImplementingType(DataFetchingEnvironment env) {
        GraphQLType outputType = env.getFieldType();

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
