package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class BasicTest extends LocalClusterTestHarness {

    @Test
    public void testSchemaQuery() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String query = "{\n" + "  __schema {\n"
            + "    queryType {\n"
            + "      name\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ExecutionResult executionResult = graphQL.execute(query);

        Map<String, Object> result = executionResult.getData();
        assertThat(result.get("__schema")).isNotNull();
    }
}
