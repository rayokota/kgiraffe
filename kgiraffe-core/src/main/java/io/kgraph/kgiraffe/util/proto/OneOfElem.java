package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.OneOfElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class OneOfElem {
    public final String name;
    public final List<FieldElem> fields;
    public final List<GroupElem> groups;
    public final List<OptionElem> options;

    public OneOfElem(OneOfElement elem) {
        name = elem.getName();
        fields = elem.getFields().stream()
            .map(FieldElem::new)
            .collect(Collectors.toList());
        groups = elem.getGroups().stream()
            .map(GroupElem::new)
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }
}