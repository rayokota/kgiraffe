package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.OneOfElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class OneOfElem {
    public String name;
    public List<FieldElem> fields;
    public List<GroupElem> groups;
    public List<OptionElem> options;

    public OneOfElem(OneOfElement elem) {
        name = elem.getName();
        fields = elem.getFields().stream()
            .map(o -> new FieldElem(o))
            .collect(Collectors.toList());
        groups = elem.getGroups().stream()
            .map(o -> new GroupElem(o))
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
    }
}