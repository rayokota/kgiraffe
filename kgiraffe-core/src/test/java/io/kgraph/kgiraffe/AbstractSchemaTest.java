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
            "  str(value: \"howdy\") {\n" +
            "    value \n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> str = (Map<String, Object>) result.get("str");
        String v = (String) str.get("value");
        assertThat(v).isEqualTo("howdy");

        String query = "query {\n" +
            "  str (where: {value: {_eq: \"howdy\"}}, " +
            "      offset: 0, limit: 1000,\n" +
            "      order_by: {value: asc}) {\n" +
            "    value \n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> strs = (List<Map<String, Object>>) result.get("str");
        str = strs.get(0);
        v = (String) str.get("value");
        assertThat(v).isEqualTo("howdy");

        mutation = "mutation {\n" +
            "  t1(value: { f1: \"hello\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
        Map<String, Object> value = (Map<String, Object>) t1.get("value");
        String f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");

        query = "query {\n" +
            "  t1 (where: {value: {f1: {_eq: \"hello\"}}}, " +
            "      offset: 0, limit: 1000,\n" +
            "      order_by: {value: {f1: asc}}) {\n" +
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
            "  t1(headers: { mykey: \"myvalue\" }, value: { f1: \"world\"}) {\n" +
            "    headers \n" +
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
        Map<String, Object> headers = (Map<String, Object>) t1.get("headers");
        String val = (String) headers.get("mykey");
        assertThat(val).isEqualTo("myvalue");

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

    @Test
    @SuppressWarnings("unchecked")
    public void testRef() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  ref(value: { f2: \"hello\"}) {\n" +
            "    value {\n" +
            "      f2\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> ref = (Map<String, Object>) result.get("ref");
        Map<String, Object> value = (Map<String, Object>) ref.get("value");
        String f2 = (String) value.get("f2");
        assertThat(f2).isEqualTo("hello");

        String query = "query {\n" +
            "  ref (where: {value: {f2: {_eq: \"hello\"}}}) {\n" +
            "    value {\n" +
            "      f2\n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> refs = (List<Map<String, Object>>) result.get("ref");
        ref = refs.get(0);
        value = (Map<String, Object>) ref.get("value");
        f2 = (String) value.get("f2");
        assertThat(f2).isEqualTo("hello");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRefById() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  refbyid(value: { f2: \"hello\"}) {\n" +
            "    value {\n" +
            "      f2\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> ref = (Map<String, Object>) result.get("refbyid");
        Map<String, Object> value = (Map<String, Object>) ref.get("value");
        String f2 = (String) value.get("f2");
        assertThat(f2).isEqualTo("hello");

        String query = "query {\n" +
            "  refbyid (where: {value: {f2: {_eq: \"hello\"}}}) {\n" +
            "    value {\n" +
            "      f2\n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> refs = (List<Map<String, Object>>) result.get("refbyid");
        ref = refs.get(0);
        value = (Map<String, Object>) ref.get("value");
        f2 = (String) value.get("f2");
        assertThat(f2).isEqualTo("hello");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRoot() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  root(value: { f1: \"hello\", nested: {f2: \"world\"}}) {\n" +
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
        Map<String, Object> root = (Map<String, Object>) result.get("root");
        Map<String, Object> value = (Map<String, Object>) root.get("value");
        String f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");
        Map<String, Object> nested = (Map<String, Object>) value.get("nested");
        String f2 = (String) nested.get("f2");
        assertThat(f2).isEqualTo("world");

        String query = "query {\n" +
            "  root (where: {value: {nested: {f2: {_eq: \"world\"}}}}) {\n" +
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
        List<Map<String, Object>> roots = (List<Map<String, Object>>) result.get("root");
        root = roots.get(0);
        value = (Map<String, Object>) root.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");
    }

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

    @Test
    @SuppressWarnings("unchecked")
    public void testTypes() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            "  types(value: { " +
            "    mynull: null," +
            "    myint: 123," +
            "    mynumericlong: 456," +
            "    mystringlong: \"789\"," +
            "    myfloat: 1.23," +
            "    mydouble: 4.56," +
            "    myboolean: true," +
            "    mystring: \"hi\"," +
            "    mybinary: \"aGk=\"," +
            "    mysuit: SPADES," +
            "    mysuit2: HEARTS," +
            "    myarray: [ \"hello\", \"world\" ]," +
            "    mymap: { key: \"value\" }" +
            "  } ) {\n" +
            "    value {\n" +
            "      mynull\n" +
            "      myint\n" +
            "      mynumericlong\n" +
            "      mystringlong\n" +
            "      myfloat\n" +
            "      mydouble\n" +
            "      mystring\n" +
            "      mybinary\n" +
            "      mysuit\n" +
            "      mysuit2\n" +
            "      myarray\n" +
            "      mymap\n" +
            "    }\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> type = (Map<String, Object>) result.get("types");
        Map<String, Object> value = (Map<String, Object>) type.get("value");
        int val = (Integer) value.get("myint");
        assertThat(val).isEqualTo(123);

        String query = "query {\n" +
            "  types (where: {value: {_and: [" +
            "    {mynull: {_eq: null}}," +
            "    {myint: {_eq: 123}}," +
            "    {mynumericlong: {_eq: 456}}," +
            "    {mystringlong: {_eq: \"789\"}}," +
            "    {myfloat: {_eq: 1.23}}," +
            "    {mydouble: {_eq: 4.56}}," +
            "    {mystring: {_eq: \"hi\"}}," +
            "    {mybinary: {_eq: \"aGk=\"}}," +
            "    {mysuit: {_eq: SPADES}}," +
            "    {mysuit2: {_eq: HEARTS}}," +
            "    {myarray: {_eq: [ \"hello\", \"world\" ]}}," +
            "    {mymap: {_eq: { key: \"value\" }}}" +
            "  ]}}) {\n" +
            "    value {\n" +
            "      mynull\n" +
            "      myint\n" +
            "      mynumericlong\n" +
            "      mystringlong\n" +
            "      myfloat\n" +
            "      mydouble\n" +
            "      mystring\n" +
            "      mybinary\n" +
            "      mysuit\n" +
            "      mysuit2\n" +
            "      myarray\n" +
            "      mymap\n" +
            "    }\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> types = (List<Map<String, Object>>) result.get("types");
        type = types.get(0);
        value = (Map<String, Object>) type.get("value");
        val = (Integer) value.get("myint");
        assertThat(val).isEqualTo(123);
    }

    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.TOPICS_CONFIG, "str,t1,t2,ref,refbyid,root,types,cycle");
    }
}
