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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A wrapper for the {@link graphql.schema.GraphQLSchema.Builder}.
 */
public class GraphQLSchemaBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphQLSchemaBuilder.class);

    public static final String QUERY_ROOT = "query_root";

    public static final String DELETED_PARAM_NAME = "deleted";
    public static final String LIMIT_PARAM_NAME = "limit";
    public static final String OFFSET_PARAM_NAME = "offset";
    public static final String ORDER_BY_PARAM_NAME = "order_by";
    public static final String TAGS_PARAM_NAME = "tags";
    public static final String WHERE_PARAM_NAME = "where";

    public static final String SCHEMA_ATTR_NAME = "schema";
    public static final String STATUS_ATTR_NAME = "status";
    public static final String TAGS_ATTR_NAME = "tags";

    public static final String TENANT = "tenant";

    private final Map<String, GraphQLImplementingType> entityCache = new HashMap<>();
    private final Map<String, GraphQLType> typeCache = new HashMap<>();
    private final Map<String, GraphQLArgument> whereArgumentsMap = new HashMap<>();
    private final Map<String, GraphQLInputType> whereAttributesMap = new HashMap<>();
    private final Map<String, GraphQLArgument> orderByArgumentsMap = new HashMap<>();

    private static final GraphQLEnumType orderByDirectionEnum =
        GraphQLEnumType.newEnum()
            .name("order_by_enum")
            .description("Specifies the direction (ascending/descending) for sorting a field")
            .value("asc", "asc", "Ascending")
            .value("desc", "desc", "Descending")
            .build();

    private static final GraphQLEnumType statusEnum =
        GraphQLEnumType.newEnum()
            .name("status_enum")
            .description("Specifies the status (active/deleted) for an entity")
            .value("active", "active", "Active")
            .value("deleted", "deleted", "Deleted")
            .build();

    private static final GraphQLInputObjectType betweenObject =
        GraphQLInputObjectType.newInputObject()
            .name("between_start_end")
            .description("Specifies the start and end for a range query")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("start")
                .description("The range start")
                .type(JavaScalars.GraphQLDate)
                .build()
            )
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("end")
                .description("The range end")
                .type(JavaScalars.GraphQLDate)
                .build()
            )
            .build();

    private static final GraphQLEnumType sinceEnum =
        GraphQLEnumType.newEnum()
            .name("since_enum")
            .description("Specifies the enum for a since query")
            .value("last_7_days", "last_7_days", "Last 7 days")
            .value("last_30_days", "last_30_days", "Last 30 days")
            .value("last_month", "last_month", "Last month")
            .value("this_month", "this_month", "This month")
            .value("today", "today", "Today")
            .value("yesterday", "yesterday", "Yesterday")
            .value("this_year", "this_year", "This year")
            .value("last_year", "last_year", "Last year")
            .value("this_quarter", "this_quarter", "This quarter")
            .value("last_quarter", "last_quarter", "Last quarter")
            .value("last_3_months", "last_3_months", "Last 3 months")
            .value("last_6_months", "last_6_months", "Last 6 months")
            .value("last_12_months", "last_12_months", "Last 12 months")
            .build();

    public GraphQLSchemaBuilder() {
    }

    /**
     * @return A freshly built {@link graphql.schema.GraphQLSchema}
     */
    public GraphQLSchema getGraphQLSchema() {
        GraphQLSchema.Builder schema = GraphQLSchema.newSchema()
            .query(getQueryType());
        return schema.build();
    }

    private GraphQLObjectType getQueryType() {
        return null;
    }

  /*
  private GraphQLObjectType getQueryType() {
    Set<String> allSuperTypes = typeRegistry.getAllEntityTypes().stream()
        .flatMap(e -> e.getSuperTypes().stream())
        .collect(Collectors.toSet());

    // TODO use this instead of deprecated dataFetcher/typeResolver methods
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

    GraphQLObjectType.Builder queryType = GraphQLObjectType.newObject()
        .name(QUERY_ROOT)
        .description("GraphQL schema for all catalog entities");
    queryType.fields(typeRegistry.getAllEntityTypes().stream()
        .filter(this::isNotIgnored)
        .map(e -> getQueryFieldDefinition(codeRegistry, e, allSuperTypes.contains(e.getTypeName())))
        .collect(Collectors.toList()));

    codeRegistry.build();

    return queryType.build();
  }

  private GraphQLFieldDefinition getQueryFieldDefinition(
      GraphQLCodeRegistry.Builder codeRegistry, AtlasEntityType entityType, boolean isSuperType) {
    final GraphQLImplementingType implementingType =
        getImplementingType(codeRegistry, entityType, isSuperType);

    GraphQLQueryFactory queryFactory = GraphQLQueryFactory.builder()
        .withDiscoveryService(discoveryService)
        .withEntityType(entityType)
        .withImplementingType(implementingType)
        .build();

    return GraphQLFieldDefinition.newFieldDefinition()
        .name(entityType.getTypeName())
        .description(entityType.getEntityDef().getDescription())
        .type(new GraphQLList(implementingType))
        .dataFetcher(new EntityFetcher(queryFactory))
        .argument(getWhereArgument(entityType))
        .argument(getTagsArgument(entityType))
        .argument(getLimitArgument(entityType))
        .argument(getOffsetArgument(entityType))
        .argument(getOrderByArgument(entityType))
        .argument(getDeletedArgument(entityType))
        .build();
  }

  private GraphQLArgument getWhereArgument(AtlasEntityType entityType) {
    return whereArgumentsMap.computeIfAbsent(entityType.getTypeName(),
        k -> computeWhereArgument(entityType));
  }

  private GraphQLArgument computeWhereArgument(AtlasEntityType entityType) {
    String typeName = entityType.getTypeName() + "_criteria";

    GraphQLInputObjectType whereInputObject = GraphQLInputObjectType.newInputObject()
        .name(typeName)
        .description("Where logical AND specification of the provided list of criteria expressions")
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Logical.OR.symbol())
            .description("Logical operation for expressions")
            .type(new GraphQLList(new GraphQLTypeReference(typeName)))
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Logical.AND.symbol())
            .description("Logical operation for expressions")
            .type(new GraphQLList(new GraphQLTypeReference(typeName)))
            .build()
        )
        .fields(entityType.getAllAttributes().values().stream()
            .filter(this::isNotIgnored)
            .flatMap(attr -> getWhereInputField(entityType, attr))
            .collect(Collectors.toList())
        )
        .build();

    return GraphQLArgument.newArgument()
        .name(WHERE_PARAM_NAME)
        .description("Where logical specification")
        .type(whereInputObject)
        .build();

  }

  private Stream<GraphQLInputObjectField> getWhereInputField(
      AtlasEntityType entityType, AtlasAttribute attribute) {
    return getWhereAttributeType(entityType, attribute)
        .filter(Objects::nonNull)
        .map(type -> GraphQLInputObjectField.newInputObjectField()
            .name(attribute.getName())
            .description(attribute.getAttributeDef().getDescription())
            .type(type)
            .build());
  }

  private Stream<GraphQLInputType> getWhereAttributeType(
      AtlasEntityType entityType, AtlasAttribute attribute) {
    String typeName = entityType.getTypeName() + "_" + attribute.getName() + "_criteria";

    if (whereAttributesMap.containsKey(typeName)) {
      return Stream.of(whereAttributesMap.get(typeName));
    }

    AtlasType type = attribute.getAttributeType();
    if (type instanceof AtlasArrayType || type instanceof AtlasMapType) {
      // ignore non-primitives
      return Stream.empty();
    }
    GraphQLType attributeType = getBasicAttributeType(type);
    if (attributeType == null) {
      return Stream.empty();
    }

    GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
        .name(typeName)
        .description("Criteria expression specification of "
            + attribute.getName() + " attribute in entity " + entityType.getTypeName())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Logical.OR.symbol())
            .description("Logical OR criteria expression")
            .type(new GraphQLList(new GraphQLTypeReference(typeName)))
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Logical.AND.symbol())
            .description("Logical AND criteria expression")
            .type(new GraphQLList(new GraphQLTypeReference(typeName)))
            .build()
       )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Criteria.EQ.symbol())
            .description("Equals criteria")
            .type((GraphQLInputType) attributeType)
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Criteria.LTE.symbol())
            .description("Less than or Equals criteria")
            .type((GraphQLInputType) attributeType)
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Criteria.GTE.symbol())
            .description("Greater or Equals criteria")
            .type((GraphQLInputType) attributeType)
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Criteria.GT.symbol())
            .description("Greater Than criteria")
            .type((GraphQLInputType) attributeType)
            .build()
        )
        .field(GraphQLInputObjectField.newInputObjectField()
            .name(Criteria.LT.symbol())
            .description("Less Than criteria")
            .type((GraphQLInputType) attributeType)
            .build()
        );
        // TODO Atlas does not support IN well in SearchProcessor.toInMemoryPredicate

    if (type instanceof AtlasStringType
        || type instanceof AtlasEnumType) {
      builder.field(GraphQLInputObjectField.newInputObjectField()
              .name(Criteria.STARTS_WITH.symbol())
              .description("Starts with criteria")
              .type((GraphQLInputType) attributeType)
              .build()
          );
    } else if (type instanceof AtlasDateType) {
      builder.field(GraphQLInputObjectField.newInputObjectField()
              .name(Criteria.BETWEEN.symbol())
              .description("Between criteria")
              .type(betweenObject)
              .build()
          )
          .field(GraphQLInputObjectField.newInputObjectField()
              .name(Criteria.SINCE.symbol())
              .description("Since criteria")
              .type(sinceEnum)
              .build()
          );
    }

    GraphQLInputType answer = builder.build();

    whereAttributesMap.putIfAbsent(typeName, answer);

    return Stream.of(answer);
  }

  private GraphQLArgument getTagsArgument(AtlasEntityType entityType) {
    return GraphQLArgument.newArgument()
        .name(TAGS_PARAM_NAME)
        .description("Limit the result set to entities with the given tag")
        .type(new GraphQLList(Scalars.GraphQLString))
        .build();
  }

  private GraphQLArgument getLimitArgument(AtlasEntityType entityType) {
    return GraphQLArgument.newArgument()
        .name(LIMIT_PARAM_NAME)
        .description("Limit the result set to the given number")
        .type(Scalars.GraphQLInt)
        .build();
  }

  private GraphQLArgument getOffsetArgument(AtlasEntityType entityType) {
    return GraphQLArgument.newArgument()
        .name(OFFSET_PARAM_NAME)
        .description("Start offset for the result set")
        .type(Scalars.GraphQLInt)
        .build();
  }

  private GraphQLArgument getOrderByArgument(AtlasEntityType entityType) {
    return orderByArgumentsMap.computeIfAbsent(entityType.getTypeName(),
        k -> computeOrderByArgument(entityType));
  }

  private GraphQLArgument computeOrderByArgument(AtlasEntityType entityType) {
    String typeName = entityType.getTypeName() + "_order";

    GraphQLInputObjectType orderByInputObject = GraphQLInputObjectType.newInputObject()
        .name(typeName)
        .description("Order by attribute")
        .fields(entityType.getAllAttributes().values().stream()
            .filter(this::isNotIgnored)
            .map(attr -> getOrderByInputField(entityType, attr))
            .collect(Collectors.toList())
        )
        .build();

    return GraphQLArgument.newArgument()
        .name(ORDER_BY_PARAM_NAME)
        .description("Order by specification")
        .type(new GraphQLList(orderByInputObject))
        .build();

  }

  private GraphQLInputObjectField getOrderByInputField(
      AtlasEntityType entityType, AtlasAttribute attribute) {
    return GraphQLInputObjectField.newInputObjectField()
        .name(attribute.getName())
        .description(attribute.getAttributeDef().getDescription())
        .type(orderByDirectionEnum)
        .build();
  }

  private GraphQLArgument getDeletedArgument(AtlasEntityType entityType) {
    return GraphQLArgument.newArgument()
        .name(DELETED_PARAM_NAME)
        .description("Whether to include deleted entities")
        .type(Scalars.GraphQLBoolean)
        .build();
  }

  private GraphQLImplementingType getImplementingType(
      GraphQLCodeRegistry.Builder codeRegistry, AtlasEntityType entityType, boolean isSuperType) {
    return entityCache.computeIfAbsent(
        entityType.getTypeName(), k ->
            computeImplementingType(codeRegistry, entityType, isSuperType));
  }

  private GraphQLImplementingType computeImplementingType(
      GraphQLCodeRegistry.Builder codeRegistry, AtlasEntityType entityType, boolean isSuperType) {
    String typeName = entityType.getTypeName();

    GraphQLTypeReference[] interfaces = entityType.getAllSuperTypes()
        .stream()
        .filter(this::isNotIgnored)
        .map(GraphQLTypeReference::new)
        .toArray(GraphQLTypeReference[]::new);

    List<GraphQLFieldDefinition> attrFields = entityType.getAllAttributes().values()
        .stream()
        .filter(this::isNotIgnored)
        .flatMap(attr -> getObjectField(codeRegistry, entityType, attr))
        .collect(Collectors.toList());

    List<GraphQLFieldDefinition> relAttrFields = entityType.getRelationshipAttributes().entrySet()
        .stream()
        .filter(e -> !e.getKey().startsWith("__"))
        .flatMap(e -> e.getValue().values().stream())
        .flatMap(attr -> getObjectField(codeRegistry, entityType, attr))
        .collect(Collectors.toCollection(ArrayList::new));

    // Add the virtual relationship to the containing schema
    if (typeName.startsWith(ModelConstants.PREFIX_SR)
        && !entityType.getRelationshipAttributes().containsKey(SCHEMA_ATTR_NAME)) {
      relAttrFields.add(GraphQLFieldDefinition.newFieldDefinition()
          .name(SCHEMA_ATTR_NAME)
          .description("Schema for the entity")
          .type(new GraphQLTypeReference(SchemaAtlasTypes.SR_SCHEMA.getName()))
          .dataFetcher(new AttributeFetcher(
              typeRegistry, graph, entityRetriever, entityType, SCHEMA_ATTR_NAME))
          .build());
    }

    if (isSuperType) {
      GraphQLInterfaceType.Builder builder = GraphQLInterfaceType.newInterface()
          .name(typeName)
          .description(entityType.getEntityDef().getDescription())
          .fields(attrFields)
          .fields(relAttrFields)
          .field(GraphQLFieldDefinition.newFieldDefinition()
              .name(STATUS_ATTR_NAME)
              .description("Status of the entity")
              .type(statusEnum)
              .dataFetcher(new AttributeFetcher(
                  typeRegistry, graph, entityRetriever, entityType, STATUS_ATTR_NAME))
              .build())
          .field(GraphQLFieldDefinition.newFieldDefinition()
              .name(TAGS_ATTR_NAME)
              .description("Tags for the entity")
              .type(new GraphQLList(Scalars.GraphQLString))
              .dataFetcher(new AttributeFetcher(
                  typeRegistry, graph, entityRetriever, entityType, TAGS_ATTR_NAME))
              .build())
          .typeResolver(new EntityResolver());
      for (GraphQLTypeReference type : interfaces) {
        builder.withInterface(type);
      }
      return builder.build();
    } else {
      return GraphQLObjectType.newObject()
          .name(typeName)
          .description(entityType.getEntityDef().getDescription())
          .fields(attrFields)
          .fields(relAttrFields)
          .field(GraphQLFieldDefinition.newFieldDefinition()
              .name(STATUS_ATTR_NAME)
              .description("Status of the entity")
              .type(statusEnum)
              .dataFetcher(new AttributeFetcher(
                  typeRegistry, graph, entityRetriever, entityType, STATUS_ATTR_NAME))
              .build())
          .field(GraphQLFieldDefinition.newFieldDefinition()
              .name(TAGS_ATTR_NAME)
              .description("Tags for the entity")
              .type(new GraphQLList(Scalars.GraphQLString))
              .dataFetcher(new AttributeFetcher(
                  typeRegistry, graph, entityRetriever, entityType, TAGS_ATTR_NAME))
              .build())
          .withInterfaces(interfaces)
          .build();
    }
  }

  private Stream<GraphQLFieldDefinition> getObjectField(
      GraphQLCodeRegistry.Builder codeRegistry,
      AtlasEntityType entityType,
      AtlasAttribute attribute) {
    return getAttributeType(entityType, attribute)
        .filter(type -> type instanceof GraphQLOutputType)
        .map(type -> GraphQLFieldDefinition.newFieldDefinition()
            .name(attribute.getName())
            .description(getAttributeDescription(attribute))
            .type((GraphQLOutputType) type)
            .dataFetcher(new AttributeFetcher(
                typeRegistry, graph, entityRetriever, entityType, attribute.getName()))
            //.arguments(arguments)
            .build());
  }

  private GraphQLType getBasicAttributeType(AtlasType type) {
    if (type instanceof AtlasStringType) {
      return Scalars.GraphQLString;
    } else if (type instanceof AtlasByteType) {
      return ExtendedScalars.GraphQLByte;
    } else if (type instanceof AtlasIntType) {
      return Scalars.GraphQLInt;
    } else if (type instanceof AtlasShortType) {
      return ExtendedScalars.GraphQLShort;
    } else if (type instanceof AtlasFloatType || type instanceof AtlasDoubleType) {
      return Scalars.GraphQLFloat;
    } else if (type instanceof AtlasLongType) {
      return ExtendedScalars.GraphQLLong;
    } else if (type instanceof AtlasBooleanType) {
      return Scalars.GraphQLBoolean;
    } else if (type instanceof AtlasBigDecimalType) {
      return ExtendedScalars.GraphQLBigDecimal;
    } else if (type instanceof AtlasBigIntegerType) {
      return ExtendedScalars.GraphQLBigInteger;
    } else if (type instanceof AtlasDateType) {
      return JavaScalars.GraphQLDate;
    } else if (type instanceof AtlasEnumType) {
      return getTypeFromJavaType(type);
    } else if (type instanceof AtlasArrayType) {
      return getTypeFromJavaType(type);
    } else if (type instanceof AtlasMapType) {
      return getTypeFromJavaType(type);
    }

    throw new UnsupportedOperationException(
        "Class could not be mapped to GraphQL: '" + type.getClass().getTypeName() + "'");
  }

  private Stream<GraphQLType> getAttributeType(
      AtlasEntityType entityType, AtlasAttribute attribute) {
    String typeName = entityType.getTypeName();
    String relationshipName = attribute.getRelationshipName();
    if (relationshipName == null) {
      try {
        GraphQLType type = getBasicAttributeType(attribute.getAttributeType());
        return type != null ? Stream.of(type) : Stream.empty();
      } catch (UnsupportedOperationException e) {
        //fall through to the exception below
        //which is more useful because it also contains the declaring member
      }
    } else {
      AtlasRelationshipType relationshipType =
          typeRegistry.getRelationshipTypeByName(relationshipName);
      AtlasRelationshipDef relationshipDef = relationshipType.getRelationshipDef();
      AtlasRelationshipEndDef thisRel;
      AtlasRelationshipEndDef otherRel;

      if (attribute.getRelationshipEdgeDirection() == AtlasRelationshipEdgeDirection.OUT) {
        thisRel = relationshipDef.getEndDef1();
        otherRel = relationshipDef.getEndDef2();
      } else {
        thisRel = relationshipDef.getEndDef2();
        otherRel = relationshipDef.getEndDef1();
      }

      if (!isNotIgnored(otherRel.getType())) {
        return Stream.empty();
      }

      switch (thisRel.getCardinality()) {
        case SINGLE:
          return Stream.of(new GraphQLTypeReference(otherRel.getType()));
        case LIST:
        case SET:
          return Stream.of(new GraphQLList(new GraphQLTypeReference(otherRel.getType())));
        default:
          throw new IllegalArgumentException();
      }
    }

    throw new UnsupportedOperationException(
        "Attribute could not be mapped to GraphQL: field '" + attribute.getName()
            + "' of entity '" + typeName + "'");
  }

  private String getAttributeDescription(AtlasAttribute attribute) {
    String relationshipName = attribute.getRelationshipName();
    if (relationshipName == null) {
      return attribute.getAttributeDef().getDescription();
    } else {
      AtlasRelationshipType relationshipType =
          typeRegistry.getRelationshipTypeByName(relationshipName);
      AtlasRelationshipDef relationshipDef = relationshipType.getRelationshipDef();
      AtlasRelationshipEndDef thisRel;

      if (attribute.getRelationshipEdgeDirection() == AtlasRelationshipEdgeDirection.OUT) {
        thisRel = relationshipDef.getEndDef1();
      } else {
        thisRel = relationshipDef.getEndDef2();
      }

      return thisRel.getDescription();
    }
  }

  private boolean isNotIgnored(AtlasEntityType entityType) {
    return isNotIgnored(entityType.getTypeName());
  }

  private boolean isNotIgnored(String typeName) {
    return ModelConstants.hasValidPrefix(typeName);
  }

  private boolean isNotIgnored(AtlasAttribute attribute) {
    // Ignore internal attributes
    return !attribute.getName().startsWith("__");
  }

  private GraphQLType getTypeFromJavaType(AtlasType type) {
    String typeName = type.getTypeName();
    if (type instanceof AtlasEnumType) {
      AtlasEnumType enumType = (AtlasEnumType) type;
      if (typeCache.containsKey(typeName)) {
        return typeCache.get(typeName);
      }

      GraphQLEnumType.Builder enumBuilder = GraphQLEnumType.newEnum().name(typeName);
      for (AtlasEnumElementDef elementDef : enumType.getEnumDef().getElementDefs()) {
        enumBuilder.value(elementDef.getValue(), elementDef.getOrdinal());
      }
      GraphQLType answer = enumBuilder.build();

      typeCache.put(typeName, answer);

      return answer;
    } else if (type instanceof AtlasArrayType) {
      AtlasArrayType arrayType = (AtlasArrayType) type;
      try {
        return GraphQLList.list(getBasicAttributeType(arrayType.getElementType()));
      } catch (UnsupportedOperationException e) {
        // Lists of entity types not supported
        return null;
      }
    } else if (type instanceof AtlasMapType) {
      return ExtendedScalars.Object;
    }

    throw new UnsupportedOperationException(
        "Class could not be mapped to GraphQL: '" + type.getClass().getTypeName() + "'");
  }
  */

}
