package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.CapturingSubscriber;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSchemaTest extends LocalClusterTestHarness {

    @Test
    @SuppressWarnings("unchecked")
    public void testSimple() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  t1(value: { f1: \"hello\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
        Map<String, Object> value = (Map<String, Object>) t1.get("value");
        String f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");

        String query = "query {\n" +
            "  t1 (where: {value: {f1: {_eq: \"hello\"}}}) {\n" +
            "    value {\n" +
            "      f1 \n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> t1s = (List<Map<String, Object>>) result.get("t1");
        t1 = t1s.get(0);
        value = (Map<String, Object>) t1.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");

        String subscription = "subscription {\n" +
            "  t1 {\n" +
            "    value {\n" +
            "    \tf1\n" +
            "    }\n" +
            "  }\n" +
            "}";
        executionResult = graphQL.execute(subscription);

        Publisher<ExecutionResult> msgStream = executionResult.getData();
        CapturingSubscriber<ExecutionResult> capturingSubscriber =
            new CapturingSubscriber<>();
        msgStream.subscribe(capturingSubscriber);

        mutation = "mutation {\n" +
            "  t1(value: { f1: \"world\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        t1 = (Map<String, Object>) result.get("t1");
        value = (Map<String, Object>) t1.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("world");


        List<ExecutionResult> events = capturingSubscriber.getEvents();
        assertThat(events).size().isEqualTo(1);
        executionResult = events.get(0);
        result = executionResult.getData();
        t1 = (Map<String, Object>) result.get("t1");
        value = (Map<String, Object>) t1.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("world");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNested() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  t2(value: { f1: \"hello\", nested: {f2: \"world\"}}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "      nested {\n" +
            "        f2\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> t2 = (Map<String, Object>) result.get("t2");
        Map<String, Object> value = (Map<String, Object>) t2.get("value");
        String f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");
        Map<String, Object> nested = (Map<String, Object>) value.get("nested");
        String f2 = (String) nested.get("f2");
        assertThat(f2).isEqualTo("world");

        String query = "query {\n" +
            "  t2 (where: {value: {nested: {f2: {_eq: \"world\"}}}}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "      nested {\n" +
            "        f2\n" +
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
        List<Map<String, Object>> t2s = (List<Map<String, Object>>) result.get("t2");
        t2 = t2s.get(0);
        value = (Map<String, Object>) t2.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");
    }

    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.TOPICS_CONFIG, "t1,t2");
    }

}
