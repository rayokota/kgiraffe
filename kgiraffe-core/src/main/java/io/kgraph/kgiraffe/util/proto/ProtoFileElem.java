package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ProtoFileElem {
    public final String packageName;
    public final String syntax;
    public final List<String> imports;
    public final List<String> publicImports;
    public final List<MessageElem> messages;
    public final List<EnumElem> enums;
    public final List<ServiceElem> services;
    @JsonProperty("extends")
    public final List<ExtendElem> extendDeclarations;
    public final List<OptionElem> options;

    public ProtoFileElem(ProtoFileElement elem) {
        packageName = elem.getPackageName();
        syntax = elem.getSyntax() != null ? elem.getSyntax().toString() : null;
        imports = elem.getImports();
        publicImports = elem.getPublicImports();
        messages = elem.getTypes().stream()
            .filter(t -> t instanceof MessageElement)
            .map(o -> new MessageElem((MessageElement) o))
            .collect(Collectors.toList());
        enums = elem.getTypes().stream()
            .filter(t -> t instanceof EnumElement)
            .map(o -> new EnumElem((EnumElement) o))
            .collect(Collectors.toList());
        services = elem.getServices().stream()
            .map(ServiceElem::new)
            .collect(Collectors.toList());
        extendDeclarations = elem.getExtendDeclarations().stream()
            .map(ExtendElem::new)
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
    }
}
