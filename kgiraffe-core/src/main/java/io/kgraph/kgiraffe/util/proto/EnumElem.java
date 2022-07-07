package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class EnumElem {
    public final String name;
    public final List<OptionElem> options;
    public final List<EnumConstantElem> constants;
    public final List<ReservedElem> reserveds;

    public EnumElem(EnumElement elem) {
        name = elem.getName();
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
        constants = elem.getConstants().stream()
            .map(EnumConstantElem::new)
            .collect(Collectors.toList());
        reserveds = elem.getReserveds().stream()
            .map(ReservedElem::new)
            .collect(Collectors.toList());
    }
}