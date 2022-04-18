package io.kgraph.kgiraffe;

import java.util.Properties;

public class AvroTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'" +
            ",'t2=avro:{\"type\":\"record\",\"name\":\"myrecord\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}," +
            "{\"name\":\"nested\"," +
            "\"type\":{\"type\": \"record\",\"name\":\"nested\",\"fields\":[{\"name\":\"f2\"," +
            "\"type\":\"string\"}]}}]}'");
    }
}
