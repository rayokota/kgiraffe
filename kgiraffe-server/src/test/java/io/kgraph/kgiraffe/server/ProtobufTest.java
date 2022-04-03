package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.KGiraffeConfig;

import java.util.Properties;

public class ProtobufTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=proto:message Foo "
            + "{ required string f1 = 1; }'");
    }
}
