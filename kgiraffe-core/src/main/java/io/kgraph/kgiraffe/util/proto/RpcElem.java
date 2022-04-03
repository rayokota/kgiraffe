package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.RpcElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class RpcElem {
    public String name;
    public String requestType;
    public String responseType;
    public List<OptionElem> options;

    public RpcElem(RpcElement elem) {
        name = elem.getName();
        requestType = elem.getRequestType();
        responseType = elem.getResponseType();
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
    }
}