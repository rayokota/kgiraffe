package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class EnumElem {
    public String name;
    public List<OptionElem> options;
    public List<EnumConstantElem> constants;
    public List<ReservedElem> reserveds;

    public EnumElem(EnumElement elem) {
        name = elem.getName();
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
        constants = elem.getConstants().stream()
            .map(o -> new EnumConstantElem(o))
            .collect(Collectors.toList());
        /* TODO
        reserveds = elem.getReserveds().stream()
            .map(o -> new ReservedElem(o))
            .collect(Collectors.toList());

         */
    }
}