package io.kgraph.kgiraffe;

import java.util.Properties;

public class ProtobufTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);

        String cycle =
            ",'cycle=proto:syntax = \"proto3\";\n" +
            "\n" +
            "message LinkedList {\n" +
            "    int32 value = 1;\n" +
            "    LinkedList next = 10;\n" +
            "}\n'";

        String serdes = "'t1=proto:message Foo " +
            "{ required string f1 = 1; }'," +
            "'t2=proto:message Foo { required string f1 = 1; optional Nested nested = 2; " +
            "message Nested { required string f2 = 1; } }'";
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, serdes + cycle);
    }
}
