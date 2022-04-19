package io.kgraph.kgiraffe.util.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

public class NamedProtobufSchema implements ParsedSchema {

    private String name;
    private ProtobufSchema schema;

    public NamedProtobufSchema(Descriptors.Descriptor descriptor) {
        this.name = descriptor.getFullName();
        this.schema = new ProtobufSchema(descriptor).copy(name);
    }

    public NamedProtobufSchema(Descriptors.EnumDescriptor enumDescriptor) {
        this.name = enumDescriptor.getFullName();
        this.schema = new ProtobufSchema(
            toProtoFile(enumDescriptor.getFile().toProto()),
            Collections.emptyList(),
            Collections.emptyMap())
            .copy(name);
    }

    private static ProtoFileElement toProtoFile(DescriptorProtos.FileDescriptorProto fileProto) {
        try {
            Method m = ProtobufSchema.class.getDeclaredMethod(
                "toProtoFile", DescriptorProtos.FileDescriptorProto.class);
            m.setAccessible(true); //if security settings allow this
            return (ProtoFileElement) m.invoke(null, fileProto);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String schemaType() {
        return schema.schemaType();
    }

    public String name() {
        return schema.name();
    }

    public String canonicalString() {
        return schema.canonicalString();
    }

    public List<SchemaReference> references() {
        return schema.references();
    }

    public List<String> isBackwardCompatible(ParsedSchema schema) {
        return schema.isBackwardCompatible(schema);
    }

    public Object rawSchema() {
        return schema.rawSchema();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedProtobufSchema that = (NamedProtobufSchema) o;
        return Objects.equals(schema, that.schema) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name);
    }
}
