package io.kgraph.kgiraffe.schema;

import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.Date;
import java.util.function.Function;

public class JavaScalars {

    private static final Logger LOG = LoggerFactory.getLogger(JavaScalars.class);

    public static final GraphQLScalarType GraphQLDate;
    public static final GraphQLScalarType GraphQLJsonPrimitive;
    public static final GraphQLScalarType GraphQLVoid;

    private static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    static {
        Coercing<Date, String> dateCoercing = new Coercing<>() {
            public String serialize(Object input) throws CoercingSerializeException {
                Date date;
                if (input instanceof Date) {
                    date = (Date) input;
                } else {
                    if (!(input instanceof String)) {
                        throw new CoercingSerializeException(
                            "Expected something we can convert to 'java.util.Date' but was '" + typeName(input)
                                + "'.");
                    }

                    date = this.parseOffsetDateTime(input.toString(), CoercingSerializeException::new);
                }

                try {
                    DateFormat dateFormat = new SimpleDateFormat(ISO_8601_FORMAT);
                    return dateFormat.format(date);
                } catch (DateTimeException e) {
                    throw new CoercingSerializeException(
                        "Unable to turn TemporalAccessor into OffsetDateTime because of : '" + e.getMessage()
                            + "'.");
                }
            }

            public Date parseValue(Object input) throws CoercingParseValueException {
                Date date;
                if (input instanceof Date) {
                    date = (Date) input;
                } else {
                    if (!(input instanceof String)) {
                        throw new CoercingParseValueException(
                            "Expected a 'String' but was '" + typeName(input) + "'.");
                    }

                    date = this.parseOffsetDateTime(input.toString(), CoercingParseValueException::new);
                }

                return date;
            }

            public Date parseLiteral(Object input) throws CoercingParseLiteralException {
                if (!(input instanceof StringValue)) {
                    throw new CoercingParseLiteralException(
                        "Expected AST type 'StringValue' but was '" + typeName(input) + "'.");
                } else {
                    return this.parseOffsetDateTime(((StringValue) input).getValue(),
                        CoercingParseLiteralException::new);
                }
            }

            public Value<?> valueToLiteral(Object input) {
                String s = this.serialize(input);
                return StringValue.newStringValue(s).build();
            }

            private Date parseOffsetDateTime(String s,
                                             Function<String, RuntimeException> exceptionMaker) {
                try {
                    DateFormat dateFormat = new SimpleDateFormat(ISO_8601_FORMAT);
                    return dateFormat.parse(s);
                } catch (ParseException e) {
                    throw exceptionMaker.apply(
                        "Invalid RFC3339 value : '" + s + "'. because of : '" + e.getMessage() + "'");
                }
            }
        };
        GraphQLDate = GraphQLScalarType.newScalar()
            .name("DateTime")
            .description("An RFC-3339 compliant DateTime Scalar")
            .coercing(dateCoercing)
            .build();

        Coercing<Object, Object> jsonPrimitiveCoercing = new Coercing<Object, Object>() {
            public Object serialize(Object input) throws CoercingSerializeException {
                if (input == null
                    || input instanceof String
                    || input instanceof Number
                    || input instanceof Boolean) {
                    return input;
                }
                throw new CoercingSerializeException(
                    "Expected something we can convert to a JSON primitive but was '" + typeName(input)
                        + "'.");
            }

            public Object parseValue(Object input) throws CoercingParseValueException {
                if (input == null
                    || input instanceof String
                    || input instanceof Number
                    || input instanceof Boolean) {
                    return input;
                }
                throw new CoercingParseValueException(
                    "Expected a JSON primitive but was '" + typeName(input) + "'.");
            }

            public Object parseLiteral(Object input) throws CoercingParseLiteralException {
                if (!(input instanceof Value)) {
                    throw new CoercingParseLiteralException(
                        "Expected AST type 'Value' but was '" + typeName(input) + "'."
                    );
                }
                if (input instanceof FloatValue) {
                    return ((FloatValue) input).getValue();
                }
                if (input instanceof StringValue) {
                    return ((StringValue) input).getValue();
                }
                if (input instanceof IntValue) {
                    return ((IntValue) input).getValue();
                }
                if (input instanceof BooleanValue) {
                    return ((BooleanValue) input).isValue();
                }
                if (input instanceof EnumValue) {
                    return ((EnumValue) input).getName();
                }
                throw new CoercingParseLiteralException(
                    "Expected AST type of primitive value but was '" + typeName(input) + "'.");
            }

            public Value<?> valueToLiteral(Object input) {
                if (input == null) {
                    return NullValue.newNullValue().build();
                }
                if (input instanceof String) {
                    return new StringValue((String) input);
                }
                if (input instanceof Float) {
                    return new FloatValue(BigDecimal.valueOf((Float) input));
                }
                if (input instanceof Double) {
                    return new FloatValue(BigDecimal.valueOf((Double) input));
                }
                if (input instanceof BigDecimal) {
                    return new FloatValue((BigDecimal) input);
                }
                if (input instanceof BigInteger) {
                    return new IntValue((BigInteger) input);
                }
                if (input instanceof Number) {
                    long l = ((Number) input).longValue();
                    return new IntValue(BigInteger.valueOf(l));
                }
                if (input instanceof Boolean) {
                    return new BooleanValue((Boolean) input);
                }
                throw new UnsupportedOperationException("Can't handle values of type: " + typeName(input));
            }
        };
        GraphQLJsonPrimitive = GraphQLScalarType.newScalar()
            .name("JsonPrimitive")
            .description("A JSON Primitive Scalar")
            .coercing(jsonPrimitiveCoercing)
            .build();

        Coercing<Void, Void> voidCoercing = new Coercing<>() {
            public Void serialize(Object input) throws CoercingSerializeException {
                return null;
            }

            public Void parseValue(Object input) throws CoercingParseValueException {
                return null;
            }

            public Void parseLiteral(Object input) throws CoercingParseLiteralException {
                return null;
            }
        };
        GraphQLVoid = GraphQLScalarType.newScalar()
            .name("Void")
            .description("A void value")
            .coercing(voidCoercing)
            .build();
    }

    private static String typeName(Object input) {
        if (input == null) {
            return "null";
        }
        return input.getClass().getSimpleName();
    }
}
