package io.kgraph.kgiraffe.schema;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
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
import io.vavr.control.Either;
import org.ojai.Value.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.createInputFieldOp;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.orderByEnum;

public class GraphQLProtobufConverter extends GraphQLSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQLProtobufConverter.class);

    public static final String PROTOBUF_ANY_TYPE = "google.protobuf.Any";
    public static final String PROTOBUF_TIMESTAMP_TYPE = "google.protobuf.Timestamp";
    public static final String PROTOBUF_DURATION_TYPE = "google.protobuf.Duration";
    public static final String PROTOBUF_STRUCT_TYPE = "google.protobuf.Struct";

    public static final String PROTOBUF_DOUBLE_WRAPPER_TYPE = "google.protobuf.DoubleValue";
    public static final String PROTOBUF_FLOAT_WRAPPER_TYPE = "google.protobuf.FloatValue";
    public static final String PROTOBUF_INT64_WRAPPER_TYPE = "google.protobuf.Int64Value";
    public static final String PROTOBUF_UINT64_WRAPPER_TYPE = "google.protobuf.UInt64Value";
    public static final String PROTOBUF_INT32_WRAPPER_TYPE = "google.protobuf.Int32Value";
    public static final String PROTOBUF_UINT32_WRAPPER_TYPE = "google.protobuf.UInt32Value";
    public static final String PROTOBUF_BOOL_WRAPPER_TYPE = "google.protobuf.BoolValue";
    public static final String PROTOBUF_STRING_WRAPPER_TYPE = "google.protobuf.StringValue";
    public static final String PROTOBUF_BYTES_WRAPPER_TYPE = "google.protobuf.BytesValue";

    public static final String PROTOBUF_FIELD_MASK_TYPE = "google.protobuf.FieldMask";
    public static final String PROTOBUF_EMPTY_TYPE = "google.protobuf.Empty";

    @Override
    public GraphQLInputType createInputType(SchemaContext ctx, Either<Type, ParsedSchema> schema) {
        // TODO iterate over schemaObj.getTypes
        return createInputRecord(ctx, ((ProtobufSchema) schema.get()).toDescriptor());
    }

    private GraphQLInputType createInputType(SchemaContext ctx, FieldDescriptor field) {
        if (field.isMapField()) {
            return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
        }
        GraphQLInputType type;
        switch (field.getType()) {
            case MESSAGE:
                type = createInputRecord(ctx, field.getMessageType());
                break;
            case ENUM:
                type = ctx.isOrderBy() ? orderByEnum : createInputEnum(ctx, field.getEnumType());
                break;
            case STRING:
            case BYTES:
                type = ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
                break;
            case INT32:
            case SINT32:
            case SFIXED32:
            case UINT32:
            case FIXED32:
                type = ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLInt;
                break;
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                // Protobuf maps long to a JSON string
                type = ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
                break;
            case FLOAT:
            case DOUBLE:
                type = ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLFloat;
                break;
            case BOOL:
                type = ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLBoolean;
                break;
            default:
                throw new IllegalArgumentException("Illegal type " + field.getType());
        }
        return field.isRepeated() ? new GraphQLList(type) : type;
    }

    private GraphQLInputType createInputRecord(SchemaContext ctx, Descriptor schema) {
        switch (schema.getFullName()) {
            case PROTOBUF_ANY_TYPE:
            case PROTOBUF_STRUCT_TYPE:
            case PROTOBUF_EMPTY_TYPE:
                return ctx.isOrderBy() ? orderByEnum : ExtendedScalars.Json;
            case PROTOBUF_TIMESTAMP_TYPE:
            case PROTOBUF_DURATION_TYPE:
            case PROTOBUF_FIELD_MASK_TYPE:
                return ctx.isOrderBy() ? orderByEnum : Scalars.GraphQLString;
        }
        FieldDescriptor unwrapped = unwrapType(schema);
        if (unwrapped != null) {
            return createInputType(ctx, unwrapped);
        }
        String name = ctx.qualify(schema.getFullName());
        GraphQLInputObjectType type = (GraphQLInputObjectType) typeCache.get(name);
        if (type != null) {
            return type;
        }
        try {
            List<GraphQLInputObjectField> fields = schema.getFields().stream()
                .map(f -> createInputField(ctx, schema, f))
                .collect(Collectors.toList());
            List<GraphQLInputObjectField> oneofs = schema.getRealOneofs().stream()
                .flatMap(o -> o.getFields().stream())
                .map(f -> createInputField(ctx, schema, f))
                .collect(Collectors.toList());
            GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject()
                .name(name)
                .fields(fields)
                .fields(oneofs);

            if (ctx.isRoot()) {
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
            type = builder.build();
            return type;
        } finally {
            ctx.setRoot(false);
            typeCache.put(name, type);
        }
    }

    private GraphQLInputObjectField createInputField(SchemaContext ctx,
                                                     Descriptor schema,
                                                     FieldDescriptor field) {
        String name = ctx.qualify(field.getFullName());
        GraphQLInputType fieldType = createInputType(ctx, field);
        if (ctx.isWhere() && !(fieldType instanceof GraphQLInputObjectType)) {
            fieldType = createInputFieldOp(name, fieldType);
        }
        return GraphQLInputObjectField.newInputObjectField()
            .name(jsonName(field))
            .type(fieldType)
            .build();
    }

    private GraphQLEnumType createInputEnum(SchemaContext ctx, EnumDescriptor schema) {
        return GraphQLEnumType.newEnum()
            .name(ctx.qualify(schema.getFullName()))
            .values(schema.getValues().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.getName())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    @Override
    public GraphQLOutputType createOutputType(SchemaContext ctx,
                                              Either<Type, ParsedSchema> schema) {
        return createOutputRecord(ctx, ((ProtobufSchema) schema.get()).toDescriptor());
    }

    private GraphQLOutputType createOutputType(SchemaContext ctx, FieldDescriptor field) {
        if (field.isMapField()) {
            return ExtendedScalars.Json;
        }
        GraphQLOutputType type;
        switch (field.getType()) {
            case MESSAGE:
                type = createOutputRecord(ctx, field.getMessageType());
                break;
            case ENUM:
                type = createOutputEnum(ctx, field.getEnumType());
                break;
            case STRING:
            case BYTES:
                type = Scalars.GraphQLString;
                break;
            case INT32:
            case SINT32:
            case SFIXED32:
            case UINT32:
            case FIXED32:
                type = Scalars.GraphQLInt;
                break;
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                // Protobuf maps long to a JSON string
                type = Scalars.GraphQLString;
                break;
            case FLOAT:
            case DOUBLE:
                type = Scalars.GraphQLFloat;
                break;
            case BOOL:
                type = Scalars.GraphQLBoolean;
                break;
            default:
                throw new IllegalArgumentException("Illegal type " + field.getType());
        }
        // TODO test MAP
        return field.isRepeated() ? new GraphQLList(type) : type;
    }

    private GraphQLOutputType createOutputRecord(SchemaContext ctx, Descriptor schema) {
        switch (schema.getFullName()) {
            case PROTOBUF_ANY_TYPE:
            case PROTOBUF_STRUCT_TYPE:
            case PROTOBUF_EMPTY_TYPE:
                return ExtendedScalars.Json;
            case PROTOBUF_TIMESTAMP_TYPE:
            case PROTOBUF_DURATION_TYPE:
            case PROTOBUF_FIELD_MASK_TYPE:
                return Scalars.GraphQLString;
        }
        FieldDescriptor unwrapped = unwrapType(schema);
        if (unwrapped != null) {
            return createOutputType(ctx, unwrapped);
        }
        String name = ctx.qualify(schema.getFullName());
        GraphQLObjectType type = (GraphQLObjectType) typeCache.get(name);
        if (type != null) {
            return type;
        }
        try {
            List<GraphQLFieldDefinition> fields = schema.getFields().stream()
                .map(f -> createOutputField(ctx, f))
                .collect(Collectors.toList());
            List<GraphQLFieldDefinition> oneofs = schema.getRealOneofs().stream()
                .flatMap(o -> o.getFields().stream())
                .map(f -> createOutputField(ctx, f))
                .collect(Collectors.toList());
            GraphQLObjectType.Builder builder = GraphQLObjectType.newObject()
                .name(name)
                .fields(fields);
            type = builder.build();
            return type;
        } finally {
            ctx.setRoot(false);
            typeCache.put(name, type);
        }
    }

    private GraphQLFieldDefinition createOutputField(SchemaContext ctx, FieldDescriptor field) {
        return GraphQLFieldDefinition.newFieldDefinition()
            .name(jsonName(field))
            .type(createOutputType(ctx, field))
            .dataFetcher(new AttributeFetcher(jsonName(field)))
            .build();
    }

    private GraphQLEnumType createOutputEnum(SchemaContext ctx, EnumDescriptor schema) {
        return GraphQLEnumType.newEnum()
            .name(ctx.qualify(schema.getFullName()))
            .values(schema.getValues().stream()
                .map(v -> GraphQLEnumValueDefinition.newEnumValueDefinition()
                    .name(v.getName())
                    .build())
                .collect(Collectors.toList()))
            .build();
    }

    private FieldDescriptor unwrapType(Descriptor schema) {
        switch (schema.getFullName()) {
            case PROTOBUF_STRING_WRAPPER_TYPE:
            case PROTOBUF_BYTES_WRAPPER_TYPE:
            case PROTOBUF_INT32_WRAPPER_TYPE:
            case PROTOBUF_UINT32_WRAPPER_TYPE:
            case PROTOBUF_INT64_WRAPPER_TYPE:
            case PROTOBUF_UINT64_WRAPPER_TYPE:
            case PROTOBUF_FLOAT_WRAPPER_TYPE:
            case PROTOBUF_DOUBLE_WRAPPER_TYPE:
            case PROTOBUF_BOOL_WRAPPER_TYPE:
                return schema.getFields().get(0);
            default:
                return null;
        }
    }

    private String jsonName(FieldDescriptor field) {
        DescriptorProtos.FieldDescriptorProto proto = field.toProto();
        return proto.hasJsonName() ? proto.getJsonName() : proto.getName();
    }
}
