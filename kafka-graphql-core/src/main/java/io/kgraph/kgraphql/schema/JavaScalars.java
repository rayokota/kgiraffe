package io.kgraph.kgraphql.schema;

import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.Date;
import java.util.function.Function;

public class JavaScalars {

    private static final Logger LOG = LoggerFactory.getLogger(JavaScalars.class);

    public static final GraphQLScalarType GraphQLDate;

    private static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    static {
        Coercing<Date, String> coercing = new Coercing<Date, String>() {
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
            .coercing(coercing)
            .build();
    }

    private static String typeName(Object input) {
        if (input == null) {
            return "null";
        }
        return input.getClass().getSimpleName();
    }
}
