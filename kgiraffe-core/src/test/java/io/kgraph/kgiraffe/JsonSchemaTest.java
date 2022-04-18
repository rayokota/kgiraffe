package io.kgraph.kgiraffe;

import java.util.Properties;

public class JsonSchemaTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=json:{\"type\":\"object\"," +
            "\"properties\":{\"f1\":{\"type\":\"string\"}}}'," +
            "'t2=json:{ \"type\": \"object\", \"properties\": { \"f1\": { \"type\": \"string\" }," +
            " \"nested\": { \"type\": \"object\", \"properties\": { \"f2\": { \"type\": " +
            "\"string\" } } } } }'");
    }
}
