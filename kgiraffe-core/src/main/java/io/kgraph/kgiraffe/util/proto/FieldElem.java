package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.FieldElement;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FieldElem {
    public final String label;
    public final String type;
    public final String name;
    public final String defaultValue;
    public final String jsonName;
    public final int tag;
    public final List<OptionElem> options;

    public FieldElem(FieldElement elem) {
        label = elem.getLabel() != null
            ? elem.getLabel().toString().toLowerCase(Locale.ROOT) : null;
        type = elem.getType();
        name = elem.getName();
        defaultValue = elem.getDefaultValue();
        jsonName = elem.getJsonName();
        tag = elem.getTag();
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }
}
