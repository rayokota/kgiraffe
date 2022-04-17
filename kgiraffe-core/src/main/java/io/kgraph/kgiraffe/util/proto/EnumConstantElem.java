package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class EnumConstantElem {
    public final String name;
    public final int tag;
    public final List<OptionElem> options;

    public EnumConstantElem(EnumConstantElement elem) {
        name = elem.getName();
        tag = elem.getTag();
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }
}
