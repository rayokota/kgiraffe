package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class EnumConstantElem {
    public String name;
    public int tag;
    public List<OptionElem> options;

    public EnumConstantElem(EnumConstantElement elem) {
        name = elem.getName();
        tag = elem.getTag();
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
    }
}