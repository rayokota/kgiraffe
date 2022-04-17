package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.ReservedElement;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ReservedElem {
    public final List<Object> values;

    public ReservedElem(ReservedElement elem) {
        values = elem.getValues();
    }
}
