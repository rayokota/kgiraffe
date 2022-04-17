package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.ExtendElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ExtendElem {
    public String name;
    public List<FieldElem> fields;

    public ExtendElem(ExtendElement elem) {
        name = elem.getName();
        fields = elem.getFields().stream()
            .map(FieldElem::new)
            .collect(Collectors.toList());
    }
}