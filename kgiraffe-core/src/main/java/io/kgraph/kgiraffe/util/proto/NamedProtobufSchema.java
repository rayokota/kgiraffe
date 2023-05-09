package io.kgraph.kgiraffe.util.proto;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

public class NamedProtobufSchema implements ParsedSchema {

    private final String name;
    private final ProtobufSchema schema;

    public NamedProtobufSchema(Descriptors.Descriptor descriptor) {
        this.name = descriptor.getFullName();
        this.schema = new ProtobufSchema(descriptor).copy(name);
    }

    public NamedProtobufSchema(Descriptors.EnumDescriptor enumDescriptor) {
        this.name = enumDescriptor.getFullName();
        this.schema = new ProtobufSchema(enumDescriptor, Collections.emptyList()).copy(name);
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

    public Integer version() {
        return schema.version();
    }

    public List<SchemaReference> references() {
        return schema.references();
    }

    public Metadata metadata() {
        return schema.metadata();
    }

    public RuleSet ruleSet() {
        return schema.ruleSet();
    }

    public ParsedSchema copy() {
        return schema.copy();
    }

    public ParsedSchema copy(Integer var1) {
        return schema.copy(var1);
    }

    public ParsedSchema copy(Metadata var1, RuleSet var2) {
        return schema.copy(var1, var2);
    }

    public ParsedSchema copy(Map<SchemaEntity, Set<String>> var1,
                             Map<SchemaEntity, Set<String>> var2) {
        return schema.copy(var1, var2);
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
