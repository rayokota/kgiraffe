package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ExtensionsElem {
    public final List<Object> values;

    public ExtensionsElem(ExtensionsElement elem) {
        values = elem.getValues();
    }
}
