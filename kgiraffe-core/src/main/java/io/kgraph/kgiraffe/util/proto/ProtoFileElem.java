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
    public String packageName;
    public String syntax;
    public List<String> imports;
    public List<String> publicImports;
    public List<MessageElem> messages;
    public List<EnumElem> enums;
    public List<ServiceElem> services;
    @JsonProperty("extends")
    public List<ExtendElem> extendDeclarations;
    public List<OptionElem> options;

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
            .map(o -> new ServiceElem(o))
            .collect(Collectors.toList());
        extendDeclarations = elem.getExtendDeclarations().stream()
            .map(o -> new ExtendElem(o))
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(o -> new OptionElem(o))
            .collect(Collectors.toList());
    }
}
