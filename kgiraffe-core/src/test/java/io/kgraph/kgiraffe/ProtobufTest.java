package io.kgraph.kgiraffe;

import java.util.Properties;

public class ProtobufTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=proto:message Foo " +
            "{ required string f1 = 1; }'," +
            "'t2=proto:message Foo { required string f1 = 1; optional Nested nested = 2; " +
            "message Nested { required string f2 = 1; } }'");
    }
}
