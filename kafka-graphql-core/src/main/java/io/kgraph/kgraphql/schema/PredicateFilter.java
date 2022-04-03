package io.kgraph.kgraphql.schema;

import com.google.common.collect.EnumHashBiMap;
import graphql.GraphQLContext;
import graphql.schema.DataFetchingEnvironment;

import java.util.Collection;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class PredicateFilter implements Comparable<PredicateFilter> {

    public enum Criteria {

        /**
         * less than
         */
        LT("lt"),
        /**
         * greater than
         */
        GT("gt"),
        /**
         * less than or equal
         */
        LTE("lte"),
        /**
         * greater than or equal
         */
        GTE("gte"),
        /**
         * equal
         */
        EQ("eq"),
        /**
         * beginning of string matches, case sensitive
         */
        STARTS_WITH("starts_with"),
        /**
         * in condition
         */
        IN("in"),
        /**
         * between condition
         */
        BETWEEN("between"),
        /**
         * since condition
         */
        SINCE("since");

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

    // TODO
    /*
  @SuppressWarnings("unchecked")
  public FilterCriteria toFilterCriteria(DataFetchingEnvironment environment) {
    FilterCriteria attrCriteria = new FilterCriteria();
    attrCriteria.setAttributeName(field);
    switch (criteria) {
      case LT:
        attrCriteria.setOperator(Operator.LT);
        break;
      case GT:
        attrCriteria.setOperator(Operator.GT);
        break;
      case LTE:
        attrCriteria.setOperator(Operator.LTE);
        break;
      case GTE:
        attrCriteria.setOperator(Operator.GTE);
        break;
      case EQ:
        attrCriteria.setOperator(Operator.EQ);
        break;
      case STARTS_WITH:
        attrCriteria.setOperator(Operator.STARTS_WITH);
        break;
      case IN:
        attrCriteria.setOperator(Operator.IN);
        break;
      case BETWEEN:
        attrCriteria.setOperator(Operator.TIME_RANGE);
        Map<String, Object> map = (Map<String, Object>) typedValue;
        Date start = (Date) map.get("start");
        Date end = (Date) map.get("end");
        attrCriteria.setAttributeValue(start.getTime() + "," + end.getTime());
        break;
      case SINCE:
        attrCriteria.setOperator(Operator.TIME_RANGE);
        attrCriteria.setAttributeValue(typedValue.toString().toUpperCase(Locale.ROOT));
        break;
    }

    if (attrCriteria.getAttributeValue() == null) {
      String value = getValueAsString();
      if (ATTR_QUALIFIED_NAME.equals(field)) {
        GraphQLContext context = environment.getGraphQlContext();
        String tenant = context.get(GraphQLSchemaBuilder.TENANT);
        value = QualifiedNameGenerator.ensureEntityTenantPrefix(tenant, null, value);
      }
      attrCriteria.setAttributeValue(value);
    }

    return attrCriteria;
  }

     */

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