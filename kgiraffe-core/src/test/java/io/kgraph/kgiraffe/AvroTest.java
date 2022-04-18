package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);

        String cycle =
            ",'cycle=avro:{\"type\": \"record\",\"name\": \"linked_list\",\"fields\" : "
                + "[{\"name\": \"value\", \"type\": \"int\"},"
                + "{\"name\": \"next\", \"type\": [\"null\", \"linked_list\"],\"default\" : null}]}'";

        String serdes = "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'" +
            ",'t2=avro:{\"type\":\"record\",\"name\":\"myrecord\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}," +
            "{\"name\":\"nested\"," +
            "\"type\":{\"type\": \"record\",\"name\":\"nested\",\"fields\":[{\"name\":\"f2\"," +
            "\"type\":\"string\"}]}}]}'";
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, serdes + cycle);

    }
}
