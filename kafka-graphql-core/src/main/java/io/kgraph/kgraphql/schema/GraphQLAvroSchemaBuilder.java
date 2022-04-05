package io.kgraph.kgraphql.schema;

import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphQLAvroSchemaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLAvroSchemaBuilder.class);

    private GraphQLInputType createInputType(SchemaContext ctx, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return createInputRecord(ctx, schema);
            case ENUM:
                return createInputEnum(ctx, schema);
            case ARRAY:
                return createInputArray(ctx, schema);
            case MAP:
                return createInputMap(ctx, schema);
            case UNION:
                return createInputUnion(ctx, schema);
            case FIXED:
                return createInputFixed(ctx, schema);
            default:
                return createInputPrimitive(ctx, schema);
        }
    }

    private GraphQLInputObjectType createInputRecord(SchemaContext ctx, Schema schema) {
        List<GraphQLInputObjectField> fields = schema.getFields().stream()
            .map(f -> createInputField(ctx, schema, f))
            .collect(Collectors.toList());
        return GraphQLInputObjectType.newInputObject()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .fields(fields)
            .build();
    }

    private GraphQLInputObjectField createInputField(SchemaContext ctx,
                                                     Schema schema,
                                                     Schema.Field schemaField) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(ctx.qualify(schema.getFullName() + "_" + schemaField.name()))
            .description(schemaField.doc())
            .type(createInputType(ctx, schemaField.schema()))
            .build();
    }

    private GraphQLEnumType createInputEnum(SchemaContext ctx,
                                       Schema schema) {
        return GraphQLEnumType.newEnum()
            .name(ctx.qualify(schema.getFullName()))
            .description(schema.getDoc())
            .values(schema.getEnumSymbols().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v)
                    .description(v)
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private GraphQLList createInputArray(SchemaContext ctx,
                                         Schema schema) {
        return new GraphQLList(createInputType(ctx, schema));
    }

    private GraphQLInputObjectType createInputMap(SchemaContext ctx,
                                                  Schema schema) {
        return GraphQLInputObjectField.newInputObjectField()
            .name(ctx.qualify(schema.getFullName() + "_" + schemaField.name()))
            .description(schemaField.doc())
            .type(createInputType(ctx, schemaField.schema()))
            .build();
        String qualifiedName = ctx.getQualifiedName(id,
            schema.getFullName() + "<string," + schema.getValueType() + ">");
        AtlasEntity entity = ctx.getEntity(qualifiedName);
        if (entity != null) {
            return entity;
        }
        entity = new AtlasEntity(SchemaAtlasTypes.SR_MAP.getName());

        SchemaAtlasHook.setDefaultAttrs(ctx, entity, qualifiedName, id, "");

        AtlasEntity type = createType(ctx, id, schema.getValueType());
        entity.setRelationshipAttribute("valueType", AtlasTypeUtil.toAtlasRelatedObjectId(type));
        type.setRelationshipAttribute("mapValue", AtlasTypeUtil.toAtlasRelatedObjectId(entity));

        ctx.createOrUpdate(entity);
        return entity;
    }

    private AtlasEntity createUnion(SchemaContext ctx,
                                    int id,
                                    Schema schema) {
        // Use an index counter as we are not using a scope to calculate unique names
        String qualifiedName = ctx.getQualifiedName(id,
            schema.getFullName() + ctx.incrementNameIndex());
        AtlasEntity entity = ctx.getEntity(qualifiedName);
        if (entity != null) {
            return entity;
        }
        entity = new AtlasEntity(SchemaAtlasTypes.SR_COMBINED.getName());

        SchemaAtlasHook.setDefaultAttrs(ctx, entity, qualifiedName, id, "");
        entity.setAttribute("kind", "oneof");

        List<AtlasEntity> types = schema.getTypes().stream()
            .map(s -> createType(ctx, id, s))
            .collect(Collectors.toList());
        entity.setRelationshipAttribute("types", AtlasTypeUtil.toAtlasRelatedObjectIds(types));
        for (AtlasEntity type : types) {
            type.setRelationshipAttribute("combined", AtlasTypeUtil.toAtlasRelatedObjectId(entity));
        }

        ctx.createOrUpdate(entity);
        return entity;
    }

    private AtlasEntity createFixed(SchemaContext ctx,
                                    int id,
                                    Schema schema) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        AtlasEntity entity = ctx.getEntity(qualifiedName);
        if (entity != null) {
            return entity;
        }
        entity = new AtlasEntity(SchemaAtlasTypes.SR_FIXED.getName());

        SchemaAtlasHook.setDefaultAttrs(ctx, entity, qualifiedName, id, schema.getName());
        entity.setAttribute("namespace", getNamespace(schema));
        entity.setAttribute("doc", schema.getDoc());
        entity.setAttribute("aliases", getAliases(schema));
        entity.setAttribute("size", schema.getFixedSize());

        ctx.createOrUpdate(entity);
        return entity;
    }

    private AtlasEntity createPrimitive(SchemaContext ctx,
                                        int id,
                                        Schema schema) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        AtlasEntity entity = ctx.getEntity(qualifiedName);
        if (entity != null) {
            return entity;
        }
        entity = new AtlasEntity(SchemaAtlasTypes.SR_PRIMITIVE.getName());

        SchemaAtlasHook.setDefaultAttrs(ctx, entity, qualifiedName, id, "");
        entity.setAttribute("type", schema.getType().getName());

        ctx.createOrUpdate(entity);
        return entity;
    }

    public void deleteSchema(SchemaContext ctx,
                             int id,
                             Schema schema,
                             boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id);
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_SCHEMA.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        deleteType(ctx, id, schema, purge);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteType(SchemaContext ctx,
                            int id,
                            Schema schema,
                            boolean purge) {
        switch (schema.getType()) {
            case RECORD:
                deleteRecord(ctx, id, schema, purge);
                break;
            case ENUM:
                deleteEnum(ctx, id, schema, purge);
                break;
            case ARRAY:
                deleteArray(ctx, id, schema, purge);
                break;
            case MAP:
                deleteMap(ctx, id, schema, purge);
                break;
            case UNION:
                deleteUnion(ctx, id, schema, purge);
                break;
            case FIXED:
                deleteFixed(ctx, id, schema, purge);
                break;
            default:
                deletePrimitive(ctx, id, schema, purge);
                break;
        }
    }

    private void deleteRecord(SchemaContext ctx,
                              int id,
                              Schema schema,
                              boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        if (ctx.isDeleted(qualifiedName)) {
            return;
        }
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_RECORD.getName(), ATTR_QUALIFIED_NAME, qualifiedName);
        ctx.addDeleted(qualifiedName);

        schema.getFields().forEach(f -> deleteField(ctx, id, schema, f, purge));

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteField(SchemaContext ctx,
                             int id,
                             Schema schema,
                             Schema.Field schemaField,
                             boolean purge) {
        String qualifiedName =
            ctx.getQualifiedName(id, schema.getFullName() + "." + schemaField.name());
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_FIELD.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        deleteType(ctx, id, schemaField.schema(), purge);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteEnum(SchemaContext ctx,
                            int id,
                            Schema schema,
                            boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_ENUM.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteArray(SchemaContext ctx,
                             int id,
                             Schema schema,
                             boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id,
            schema.getFullName() + "<" + schema.getElementType() + ">");
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_ARRAY.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        deleteType(ctx, id, schema.getElementType(), purge);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteMap(SchemaContext ctx,
                           int id,
                           Schema schema,
                           boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id,
            schema.getFullName() + "<string," + schema.getValueType() + ">");
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_MAP.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        deleteType(ctx, id, schema.getValueType(), purge);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteUnion(SchemaContext ctx,
                             int id,
                             Schema schema,
                             boolean purge) {
        // Use an index counter as we are not using a scope to calculate unique names
        String qualifiedName = ctx.getQualifiedName(id,
            schema.getFullName() + ctx.incrementNameIndex());
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_COMBINED.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        schema.getTypes().forEach(s -> deleteType(ctx, id, s, purge));

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deleteFixed(SchemaContext ctx,
                             int id,
                             Schema schema,
                             boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_FIXED.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private void deletePrimitive(SchemaContext ctx,
                                 int id,
                                 Schema schema,
                                 boolean purge) {
        String qualifiedName = ctx.getQualifiedName(id, schema.getFullName());
        AtlasObjectId entityId = new AtlasObjectId(
            SchemaAtlasTypes.SR_PRIMITIVE.getName(), ATTR_QUALIFIED_NAME, qualifiedName);

        if (purge) {
            ctx.purge(entityId);
        } else {
            ctx.delete(entityId);
        }
    }

    private String getNamespace(Schema schema) {
        try {
            return schema.getNamespace();
        } catch (AvroRuntimeException e) {
            return "";
        }
    }

    private Set<String> getAliases(Schema schema) {
        try {
            return schema.getAliases();
        } catch (AvroRuntimeException e) {
            return Collections.emptySet();
        }
    }
}
