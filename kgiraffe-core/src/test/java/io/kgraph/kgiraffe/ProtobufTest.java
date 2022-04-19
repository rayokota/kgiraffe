package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufTest extends AbstractSchemaTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testMulti() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  multi(value: { Message2: {i2: \"123\"}}) {\n" +
            "    value {\n" +
            "      Message2 {\n" +
            "        i2\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> multi = (Map<String, Object>) result.get("multi");
        Map<String, Object> value = (Map<String, Object>) multi.get("value");
        Map<String, Object> msg = (Map<String, Object>) value.get("Message2");
        String i2 = (String) msg.get("i2");
        assertThat(i2).isEqualTo("123");

        String query = "query {\n" +
            "  multi (where: {value: {Message2: {i2: {_eq: \"123\"}}}}) {\n" +
            "    value {\n" +
            "      Message2 {\n" +
            "        i2\n" +
            "      }\n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> multis = (List<Map<String, Object>>) result.get("multi");
        multi = multis.get(0);
        value = (Map<String, Object>) multi.get("value");
        msg = (Map<String, Object>) value.get("Message2");
        i2 = (String) msg.get("i2");
        assertThat(i2).isEqualTo("123");
    }

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);

        String multi =
            ",'multi=proto:syntax = \"proto3\";\n" +
                "\n" +
                "message Message1 {\n" +
                "    int64 i1 = 1;\n" +
                "}\n" +
                "message Message2 {\n" +
                "    int64 i2 = 2;\n" +
                "}\n'";

        String cycle =
            ",'cycle=proto:syntax = \"proto3\";\n" +
            "\n" +
            "message LinkedList {\n" +
            "    int32 value = 1;\n" +
            "    LinkedList next = 10;\n" +
            "}\n'";

        String types =
            ",'types=proto:syntax = \"proto3\";\n" +
                "import \"google/protobuf/struct.proto\";\n" +
                "\n" +
                "message MyMessage {\n" +
                "    google.protobuf.NullValue mynull = 1;\n" +
                "    int32 myint = 2;\n" +
                "    sint32 mynumericlong = 3;\n" +
                "    int64 mystringlong = 4;\n" +
                "    float myfloat = 5;\n" +
                "    double mydouble = 6;\n" +
                "    bool myboolean = 7;\n" +
                "    string mystring = 8;\n" +
                "    bytes mybinary = 9;\n" +
                "    MyEnum mysuit = 10;\n" +
                "    repeated string myarray = 11;\n" +
                "    map<string, string> mymap = 12;\n" +
                "\n" +
                "    enum MyEnum {\n" +
                "        SPADES = 0;\n" +
                "        HEARTS = 1;\n" +
                "        DIAMONDS = 2;\n" +
                "        CLUBS = 3;\n" +
                "    }\n" +
                "}\n'";

        String serdes = "'t1=proto:message Foo " +
            "{ required string f1 = 1; }'," +
            "'t2=proto:message Foo { required string f1 = 1; optional Nested nested = 2; " +
            "message Nested { required string f2 = 1; } }'";

        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, serdes + types + cycle + multi);
        props.put(KGiraffeConfig.TOPICS_CONFIG, props.get(KGiraffeConfig.TOPICS_CONFIG) + ",multi");
    }
}
