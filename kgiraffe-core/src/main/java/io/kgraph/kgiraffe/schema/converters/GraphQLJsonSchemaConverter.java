package io.kgraph.kgiraffe.schema.converters;

import graphql.Scalars;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLTypeReference;
import io.kgraph.kgiraffe.schema.AttributeFetcher;
import io.kgraph.kgiraffe.schema.JavaScalars;
import io.kgraph.kgiraffe.schema.Logical;
import io.kgraph.kgiraffe.schema.SchemaContext;
import io.kgraph.kgiraffe.schema.SchemaContext.Mode;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.createInputFieldOp;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLJsonSchemaConverter extends GraphQLSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLJsonSchemaConverter.class);

    @Override
    public GraphQLInputType createInputType(SchemaContext ctx, Either<Type, ParsedSchema> schema) {
        String scope = "";
        return createInputType(ctx, scope, ((JsonSchema) schema.get()).rawSchema());
    }

    private GraphQLInputType createInputType(SchemaContext ctx, String scope, Schema schema) {
        if (schema instanceof StringSchema) {
            return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
        } else if (schema instanceof NumberSchema) {
            if (((NumberSchema) schema).requiresInteger()) {
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLInt;
            } else {
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLFloat;
            }
        } else if (schema instanceof BooleanSchema) {
            return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLBoolean;
        } else if (schema instanceof NullSchema) {
            return ctx.isOrderBy() ? orderByEnum : JavaScalars.GraphQLVoid;
        } else if (schema instanceof ConstSchema) {
            return ctx.isOrderBy() ? orderByEnum :
                createInputConst(ctx, scope, (ConstSchema) schema);
        } else if (schema instanceof EnumSchema) {
            return ctx.isOrderBy() ? orderByEnum :
                createInputEnum(ctx, scope, (EnumSchema) schema);
        } else if (schema instanceof CombinedSchema) {
            if (isOptional(schema)) {
                Schema subschema = ((CombinedSchema) schema).getSubschemas().stream()
                    .filter(s -> !(s instanceof NullSchema))
                    .findFirst().get();
                return createInputType(ctx, scope, subschema);
            } else {
                return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
            }
        } else if (schema instanceof NotSchema) {
            return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
        } else if (schema instanceof ConditionalSchema) {
            return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
        } else if (schema instanceof EmptySchema) {
            return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
        } else if (schema instanceof FalseSchema) {
            return ctx.isOrderBy() ? orderByEnum : JavaScalars.GraphQLVoid;
        } else if (schema instanceof ObjectSchema) {
            return createInputRecord(ctx, scope, (ObjectSchema) schema);
        } else if (schema instanceof ArraySchema) {
            String name = scope + "array";
            return ctx.isOrderBy() ? orderByEnum : new GraphQLList(
                createInputType(ctx, name + "_", ((ArraySchema) schema).getAllItemSchema()));
        } else if (schema instanceof ReferenceSchema) {
            String name = scope + "ref";
            return ctx.isOrderBy() ? orderByEnum :
                createInputType(ctx, name + "_", ((ReferenceSchema) schema).getReferredSchema());
        } else {
            throw new IllegalArgumentException("Illegal type " + schema.getClass().getName());
        }
    }

    private GraphQLInputType createInputRecord(SchemaContext ctx,
                                                     String scope,
                                                     ObjectSchema schema) {
        String scopedName = scope + "object";
        String name = ctx.qualify(scopedName);
        // Wrap raw schema with JsonSchema as the latter avoids recursive equals/hashCode
        String cachedName = ctx.cacheIfAbsent(new JsonSchema(schema), name);
        if (cachedName != null) {
            return new GraphQLTypeReference(cachedName);
        }
        boolean isRoot = ctx.isRoot();
        if (isRoot) {
            ctx.setRoot(false);
        }
        List<GraphQLInputObjectField> fields = schema.getPropertySchemas().entrySet().stream()
            .map(f -> createInputField(
                ctx, scopedName + "_", new Tuple2<>(f.getKey(), f.getValue())))
            .collect(Collectors.toList());
        GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
            .name(name)
            .description(schema.getDescription())
            .fields(fields);

        if (isRoot) {
            if (ctx.isWhere()) {
                builder.field(GraphQLInputObjectField.newInputObjectField()
                        .name(Logical.OR.symbol())
                        .description("Logical operation for expressions")
                        .type(new GraphQLList(new GraphQLTypeReference(name)))
                        .build())
                    .field(GraphQLInputObjectField.newInputObjectField()
                        .name(Logical.AND.symbol())
                        .description("Logical operation for expressions")
                        .type(new GraphQLList(new GraphQLTypeReference(name)))
                        .build());
            }
        }
        return builder.build();
    }

    private GraphQLInputObjectField createInputField(SchemaContext ctx,
                                                     String scope,
                                                     Tuple2<String, Schema> field) {
        String fieldName = field._1;
        String scopedName = scope + fieldName;
        String name = ctx.qualify(scopedName);
        GraphQLInputType fieldType = createInputType(ctx, scopedName + "_", field._2);
        if (ctx.isWhere()
            && !(fieldType instanceof GraphQLInputObjectType)
            && !(fieldType instanceof GraphQLTypeReference)) {
            fieldType = createInputFieldOp(name, fieldType);
        }
        return GraphQLInputObjectField.newInputObjectField()
            .name(fieldName)
            .description(field._2.getDescription())
            .type(fieldType)
            .build();
    }

    private GraphQLEnumType createInputConst(SchemaContext ctx, String scope, ConstSchema schema) {
        String scopedName = scope + "enum";
        String name = ctx.qualify(scopedName);
        return GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDescription())
            .values(Stream.of(schema.getPermittedValue())
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.toString())
                    .description(v.toString())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private GraphQLEnumType createInputEnum(SchemaContext ctx, String scope, EnumSchema schema) {
        String scopedName = scope + "enum";
        String name = ctx.qualify(scopedName);
        return GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDescription())
            .values(schema.getPossibleValues().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.toString())
                    .description(v.toString())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    @Override
    public GraphQLOutputType createOutputType(SchemaContext ctx,
                                              Either<Type, ParsedSchema> schema) {
        return createOutputType(ctx, "", ((JsonSchema) schema.get()).rawSchema());
    }

    public GraphQLOutputType createOutputType(SchemaContext ctx, String scope, Schema schema) {
        if (schema instanceof StringSchema) {
            return Scalars.GraphQLString;
        } else if (schema instanceof NumberSchema) {
            if (((NumberSchema) schema).requiresInteger()) {
                return Scalars.GraphQLInt;
            } else {
                return Scalars.GraphQLFloat;
            }
        } else if (schema instanceof BooleanSchema) {
            return Scalars.GraphQLBoolean;
        } else if (schema instanceof NullSchema) {
            return JavaScalars.GraphQLVoid;
        } else if (schema instanceof ConstSchema) {
            return createOutputConst(ctx, scope, (ConstSchema) schema);
        } else if (schema instanceof EnumSchema) {
            return createOutputEnum(ctx, scope, (EnumSchema) schema);
        } else if (schema instanceof CombinedSchema) {
            if (isOptional(schema)) {
                Schema subschema = ((CombinedSchema) schema).getSubschemas().stream()
                    .filter(s -> !(s instanceof NullSchema))
                    .findFirst().get();
                return createOutputType(ctx, scope, subschema);
            } else {
                return ExtendedScalars.Json;
            }
        } else if (schema instanceof NotSchema) {
            return ExtendedScalars.Json;
        } else if (schema instanceof ConditionalSchema) {
            return ExtendedScalars.Json;
        } else if (schema instanceof EmptySchema) {
            return ExtendedScalars.Json;
        } else if (schema instanceof FalseSchema) {
            return JavaScalars.GraphQLVoid;
        } else if (schema instanceof ObjectSchema) {
            return createOutputRecord(ctx, scope, (ObjectSchema) schema);
        } else if (schema instanceof ArraySchema) {
            String name = scope + "array";
            return new GraphQLList(
                createOutputType(ctx, name + "_", ((ArraySchema) schema).getAllItemSchema()));
        } else if (schema instanceof ReferenceSchema) {
            String name = scope + "ref";
            return createOutputType(ctx, name + "_", ((ReferenceSchema) schema).getReferredSchema());
        } else {
            throw new IllegalArgumentException("Illegal type " + schema.getClass().getName());
        }
    }

    private GraphQLOutputType createOutputRecord(SchemaContext ctx,
                                                 String scope,
                                                 ObjectSchema schema) {
        String scopedName = scope + "object";
        String name = ctx.qualify(scopedName);
        // Wrap raw schema with JsonSchema as the latter avoids recursive equals/hashCode
        String cachedName = ctx.cacheIfAbsent(new JsonSchema(schema), name);
        if (cachedName != null) {
            return new GraphQLTypeReference(cachedName);
        }
        List<GraphQLFieldDefinition> fields = schema.getPropertySchemas().entrySet().stream()
            .map(f -> createOutputField(
                ctx, scopedName + "_", new Tuple2<>(f.getKey(), f.getValue())))
            .collect(Collectors.toList());
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
            .name(name)
            .description(schema.getDescription())
            .fields(fields);
        return builder.build();
    }

    private GraphQLFieldDefinition createOutputField(SchemaContext ctx,
                                                     String scope,
                                                     Tuple2<String, Schema> field) {
        String fieldName = field._1;
        String scopedName = scope + fieldName;
        String name = ctx.qualify(scopedName);
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .description(field._2.getDescription())
            .type(createOutputType(ctx, scopedName + "_", field._2))
            .dataFetcher(new AttributeFetcher(fieldName))
            .build();
    }

    private GraphQLEnumType createOutputConst(SchemaContext ctx, String scope, ConstSchema schema) {
        String scopedName = scope + "enum";
        String name = ctx.qualify(scopedName);
        return GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDescription())
            .values(Stream.of(schema.getPermittedValue())
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.toString())
                    .description(v.toString())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private GraphQLEnumType createOutputEnum(SchemaContext ctx, String scope, EnumSchema schema) {
        String scopedName = scope + "enum";
        String name = ctx.qualify(scopedName);
        return GraphQLEnumType.newEnum()
            .name(name)
            .description(schema.getDescription())
            .values(schema.getPossibleValues().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.toString())
                    .description(v.toString())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private boolean isOptional(Schema schema) {
        if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            return combinedSchema.getSubschemas().size() == 2
                && combinedSchema.getSubschemas().stream().anyMatch(s -> s instanceof NullSchema);
        }
        return false;
    }
}
