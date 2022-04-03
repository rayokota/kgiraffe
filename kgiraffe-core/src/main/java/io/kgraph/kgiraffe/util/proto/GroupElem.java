package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.GroupElement;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class GroupElem{
    public String label;
    public String name;
    public int tag;
    public List<FieldElem> fields;

    public GroupElem(GroupElement elem) {
        label = elem.getLabel() != null
            ? elem.getLabel().toString().toLowerCase(Locale.ROOT) : null;
        name = elem.getName();
        tag = elem.getTag();
        fields = elem.getFields().stream()
            .map(o -> new FieldElem(o))
            .collect(Collectors.toList());
    }
}