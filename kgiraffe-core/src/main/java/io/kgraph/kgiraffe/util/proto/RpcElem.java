package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.RpcElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RpcElem {
    public final String name;
    public final String requestType;
    public final String responseType;
    public final List<OptionElem> options;

    public RpcElem(RpcElement elem) {
        name = elem.getName();
        requestType = elem.getRequestType();
        responseType = elem.getResponseType();
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }
}