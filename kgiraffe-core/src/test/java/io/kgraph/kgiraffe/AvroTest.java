package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroTest extends AbstractSchemaTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testCycle() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  cycle(value: { value: 123, next: null}) {\n" +
            "    value {\n" +
            "      value\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> cycle = (Map<String, Object>) result.get("cycle");
        Map<String, Object> value = (Map<String, Object>) cycle.get("value");
        int val = (Integer) value.get("value");
        assertThat(val).isEqualTo(123);

        String query = "query {\n" +
            "  cycle (where: {value: {value: {_eq: 123}}}) {\n" +
            "    value {\n" +
            "      value\n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> cycles = (List<Map<String, Object>>) result.get("cycle");
        cycle = cycles.get(0);
        value = (Map<String, Object>) cycle.get("value");
        val = (Integer) value.get("value");
        assertThat(val).isEqualTo(123);
    }

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
