package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class MessageElem {
    public final String name;
    public final List<MessageElem> messages;
    public final List<EnumElem> enums;
    public final List<OptionElem> options;
    public final List<ReservedElem> reserveds;
    public final List<FieldElem> fields;
    public final List<OneOfElem> oneofs;
    public final List<ExtensionsElem> extensions;
    public final List<GroupElem> groups;

    public MessageElem(MessageElement elem) {
        name = elem.getName();
        messages = elem.getNestedTypes().stream()
            .filter(t -> t instanceof MessageElement)
            .map(o -> new MessageElem((MessageElement) o))
            .collect(Collectors.toList());
        enums = elem.getNestedTypes().stream()
            .filter(t -> t instanceof EnumElement)
            .map(o -> new EnumElem((EnumElement) o))
            .collect(Collectors.toList());
        options = elem.getOptions().stream()
            .map(OptionElem::new)
            .collect(Collectors.toList());
        reserveds = elem.getReserveds().stream()
            .map(ReservedElem::new)
            .collect(Collectors.toList());
        fields = elem.getFields().stream()
            .map(FieldElem::new)
            .collect(Collectors.toList());
        oneofs = elem.getOneOfs().stream()
            .map(OneOfElem::new)
            .collect(Collectors.toList());
        extensions = elem.getExtensions().stream()
            .map(ExtensionsElem::new)
            .collect(Collectors.toList());
        groups = elem.getGroups().stream()
            .map(GroupElem::new)
            .collect(Collectors.toList());
    }
}