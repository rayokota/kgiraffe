package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.ServiceElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ServiceElem {
    public String name;
    public List<RpcElem> rpcs;
    public List<OptionElem> options;

    public ServiceElem(ServiceElement elem) {
        name = elem.getName();
        rpcs = elem.getRpcs().stream()
            .map(o -> new RpcElem(o))
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
    }

}