package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class AvroTest extends LocalClusterTestHarness {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testBasic() throws Exception {
        GraphQL graphQL = getEngine().getGraphQL();

        String query =
            "{\n" + "  __schema {\n"
                + "    queryType {\n"
                + "      name\n"
                + "    }\n"
                + "  }\n"
                + "}";

        ExecutionResult executionResult = graphQL.execute(query);

        Map<String, Object> result = executionResult.getData();

        System.out.println("****  " + result);
        System.out.println("****  " + result);
        System.out.println("****  " + result);
        System.out.println("****  " + result);
        System.out.println("****  " + result);

        //assertThat(getResp2.getKvs()).hasSize(1);
        //assertThat(getResp2.getKvs().get(0).getValue().toString(UTF_8)).isEqualTo(oneTwoThree.toString(UTF_8));
    }
}
