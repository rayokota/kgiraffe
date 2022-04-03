package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.KGiraffeConfig;

import java.util.Properties;

public class JsonSchemaTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=json:{\"type\":\"object\","
            + "\"properties\":{\"f1\":{\"type\":\"string\"}}}'");
    }
}
