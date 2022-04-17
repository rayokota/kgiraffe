package io.kgraph.kgiraffe.util.proto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.squareup.wire.schema.internal.parser.OptionElement;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class OptionElem {
    public String name;
    public String kind;
    public Object value;

    public OptionElem(OptionElement elem) {
        name = elem.isParenthesized() ? "(" + elem.getName() + ")" : elem.getName();
        kind = elem.getKind().toString().toLowerCase(Locale.ROOT);
        value = convertValue(elem.getValue());
    }

    @SuppressWarnings("unchecked")
    private Object convertValue(Object value) {
        if (value instanceof OptionElement) {
            return new OptionElem((OptionElement) value);
        } else if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) value;
            return convertMap(map);
        } else if (value instanceof List) {
            List<Object> list = (List<Object>) value;
            return convertList(list);
        } else if (value instanceof OptionElement.OptionPrimitive) {
            OptionElement.OptionPrimitive p = (OptionElement.OptionPrimitive) value;
            return convertValue(p.getValue());
        } else {
            return value;
        }
    }

    private List<Object> convertList(List<Object> list) {
        return list.stream()
            .map(this::convertValue)
            .collect(Collectors.toList());
    }

    private Map<String, Object> convertMap(Map<String, Object> map) {
        return map.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> convertValue(e.getValue())));
    }
}
