package io.kgraph.kgiraffe.schema;

import com.google.common.collect.EnumHashBiMap;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.HValue;
import io.hdocdb.store.HQueryCondition;
import org.ojai.store.QueryCondition.Op;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class PredicateFilter implements Comparable<PredicateFilter> {

    public enum Criteria {

        /**
         * less than
         */
        LT("_lt"),
        /**
         * greater than
         */
        GT("_gt"),
        /**
         * less than or equal
         */
        LTE("_lte"),
        /**
         * greater than or equal
         */
        GTE("_gte"),
        /**
         * equal
         */
        EQ("_eq"),
        /**
         * not equal
         */
        NEQ("_neq"),
        /**
         * in condition
         */
        IN("_in"),
        /**
         * not in condition
         */
        NIN("_nin");

        private static final EnumHashBiMap<Criteria, String> lookup =
            EnumHashBiMap.create(Criteria.class);

        static {
            for (Criteria type : Criteria.values()) {
                lookup.put(type, type.symbol());
            }
        }

        private final String symbol;

        Criteria(String symbol) {
            this.symbol = symbol;
        }

        public String symbol() {
            return symbol;
        }

        public static Criteria get(String symbol) {
            return lookup.inverse().get(symbol);
        }

        public static Set<String> symbols() {
            return lookup.inverse().keySet();
        }

        @Override
        public String toString() {
            return symbol();
        }
    }

    private final String field;

    private final Object typedValue;

    private final Criteria criteria;

    public PredicateFilter(String field, Object value, Criteria criteria) {
        this.field = field;
        this.typedValue = value;
        this.criteria = criteria;
    }

    public String getField() {
        return field;
    }

    public Object getValue() {
        return typedValue;
    }

    public Criteria getCriteria() {
        return criteria;
    }

    @Override
    public int compareTo(PredicateFilter o) {
        return this.getField().compareTo(o.getField());
    }

    public HQueryCondition toQueryCondition(DataFetchingEnvironment env) {
        HQueryCondition attrCriteria = new HQueryCondition();
        switch (criteria) {
            case LT:
                attrCriteria.is(field, Op.LESS, HValue.initFromObject(typedValue));
                break;
            case GT:
                attrCriteria.is(field, Op.GREATER, HValue.initFromObject(typedValue));
                break;
            case LTE:
                attrCriteria.is(field, Op.LESS_OR_EQUAL, HValue.initFromObject(typedValue));
                break;
            case GTE:
                attrCriteria.is(field, Op.GREATER_OR_EQUAL, HValue.initFromObject(typedValue));
                break;
            case EQ:
                attrCriteria.is(field, Op.EQUAL, HValue.initFromObject(typedValue));
                break;
            case NEQ:
                attrCriteria.is(field, Op.NOT_EQUAL, HValue.initFromObject(typedValue));
                break;
            case IN:
                attrCriteria.in(field, HValue.initFromList((List) typedValue));
                break;
            case NIN:
                attrCriteria.notIn(field, HValue.initFromList((List) typedValue));
                break;
        }
        return attrCriteria.close();
    }

    private String getValueAsString() {
        return getValueAsString(typedValue);
    }

    @SuppressWarnings("unchecked")
    private static String getValueAsString(Object typedValue) {
        if (typedValue instanceof String) {
            return (String) typedValue;
        } else if (typedValue instanceof Number) {
            return typedValue.toString();
        } else if (typedValue instanceof Date) {
            return String.valueOf(((Date) typedValue).getTime());
        } else if (typedValue instanceof Collection) {
            return ((Collection<Object>) typedValue).stream()
                .map(PredicateFilter::getValueAsString)
                .collect(Collectors.joining(" "));
        } else {
            return typedValue.toString();
        }
    }
}