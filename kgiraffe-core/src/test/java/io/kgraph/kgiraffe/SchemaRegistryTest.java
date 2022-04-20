package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaRegistryTest extends LocalClusterTestHarness {

    @Test
    public void testSchemaOperations() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String mutation = "mutation {\n" +
            " \t_stage_schema (\n" +
            "    schema_type: \"AVRO\",\n" +
            "    schema: \"{\\\"namespace\\\": \\\"ns\\\", \\\"type\\\": \\\"record\\\", " +
            "\\\"name\\\": \\\"MyRecord\\\", \\\"fields\\\": [ {\\\"name\\\": \\\"field1\\\"," +
            "\\\"type\\\": \\\"string\\\"}]}\"\n" +
            "  ) {\n" +
            "    id \n" +
            "    schema\n" +
            "  }\n" +
            "}";

        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> body = (Map<String, Object>) result.get("_stage_schema");
        Number id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-1);

        mutation = "mutation {\n" +
            " \t_stage_schema (\n" +
            "    schema_type: \"AVRO\",\n" +
            "    schema: \"{\\\"namespace\\\": \\\"ns\\\", \\\"type\\\": \\\"record\\\", " +
            "\\\"name\\\": \\\"MyRecord\\\", \\\"fields\\\": [ {\\\"name\\\": \\\"field1\\\"," +
            "\\\"type\\\": \\\"int\\\"}]}\"\n" +
            "  ) {\n" +
            "    id \n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_stage_schema");
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-2);

        mutation = "mutation {\n" +
            " \t_stage_schema (\n" +
            "    schema_type: \"AVRO\",\n" +
            "    schema: \"{\\\"namespace\\\": \\\"ns\\\", \\\"type\\\": \\\"record\\\", " +
            "\\\"name\\\": \\\"MyRecord\\\", \\\"fields\\\": [ {\\\"name\\\": \\\"field1\\\"," +
            "\\\"type\\\": \\\"badint\\\"}]}\"\n" +
            "  ) {\n" +
            "    id \n" +
            "    schema\n" +
            "    validation_error\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_stage_schema");
        id = (Number) body.get("id");
        String validationError = (String) body.get("validation_error");
        assertThat(id.intValue()).isEqualTo(-3);
        assertThat(validationError).isNotNull();

        String query = "query {\n" +
            "  _test_schema_compatibility (next_id: -2, prev_id: -1) {\n" +
            "    is_backward_compatible\n" +
            "    compatibility_errors\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_test_schema_compatibility");
        Boolean isBackwardCompatible = (Boolean) body.get("is_backward_compatible");
        assertThat(isBackwardCompatible.booleanValue()).isEqualTo(false);

        query = "query {\n" +
            "  _query_staged_schemas(where: {id: {_eq: -1}}) {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> bodies =
            (List<Map<String, Object>>) result.get("_query_staged_schemas");
        body = bodies.get(0);
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-1);

        query = "query {\n" +
            "  _query_staged_schemas(where: {id: {_eq: -3}}) {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        bodies = (List<Map<String, Object>>) result.get("_query_staged_schemas");
        body = bodies.get(0);
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-3);

        mutation = "mutation {\n" +
            "  _register_schema(id: -1, subject: \"new-subject\") {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_register_schema");
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(1);

        query = "query {\n" +
            "  _query_registered_schemas(id: 1) {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        bodies = (List<Map<String, Object>>) result.get("_query_registered_schemas");
        body = bodies.get(0);
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(1);

        query = "query {\n" +
            "  _query_registered_schemas(version: 1, subject: \"new-subject\") {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        bodies = (List<Map<String, Object>>) result.get("_query_registered_schemas");
        body = bodies.get(0);
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(1);

        query = "query {\n" +
            "  _query_registered_schemas(subject: \"new-subject\") {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        bodies = (List<Map<String, Object>>) result.get("_query_registered_schemas");
        body = bodies.get(0);
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(1);

        query = "query {\n" +
            "  _query_subjects \n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<String> subjects = (List<String>) result.get("_query_subjects");
        String subject = subjects.get(0);
        assertThat(subject).isEqualTo("new-subject");

        query = "query {\n" +
            "  _test_schema_compatibility (next_id: -2, prev_subject: \"new-subject\") {\n" +
            "    is_backward_compatible\n" +
            "    compatibility_errors\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_test_schema_compatibility");
        isBackwardCompatible = (Boolean) body.get("is_backward_compatible");
        assertThat(isBackwardCompatible.booleanValue()).isEqualTo(false);

        mutation = "mutation {\n" +
            "  _unstage_schema(id: -2) {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_unstage_schema");
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-2);

        mutation = "mutation {\n" +
            "  _unstage_schema(id: -3) {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(mutation);
        result = executionResult.getData();
        body = (Map<String, Object>) result.get("_unstage_schema");
        id = (Number) body.get("id");
        assertThat(id.intValue()).isEqualTo(-3);

        query = "query {\n" +
            "  _query_staged_schemas {\n" +
            "    id\n" +
            "    schema\n" +
            "  }\n" +
            "}";

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        bodies =
            (List<Map<String, Object>>) result.get("_query_staged_schemas");
        assertThat(bodies).isEmpty();
    }
}
