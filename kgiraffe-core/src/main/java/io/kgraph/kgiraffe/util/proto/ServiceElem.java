package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.ServiceElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ServiceElem {
    public final String name;
    public final List<RpcElem> rpcs;
    public final List<OptionElem> options;

    public ServiceElem(ServiceElement elem) {
        name = elem.getName();
        rpcs = elem.getRpcs().stream()
            .map(RpcElem::new)
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }

}