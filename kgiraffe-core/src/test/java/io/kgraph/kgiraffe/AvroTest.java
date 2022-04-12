package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.http.HttpClient;
import io.vertx.rxjava3.ext.web.client.HttpResponse;
import io.vertx.rxjava3.ext.web.client.WebClient;
import io.vertx.rxjava3.ext.web.client.predicate.ResponsePredicate;
import io.vertx.rxjava3.ext.web.codec.BodyCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AvroTest extends LocalClusterTestHarness {

    private WebClient webClient;

    @BeforeEach
    public void setUp(Vertx vertx, VertxTestContext testContext) throws Exception {
        super.setUp(vertx, testContext);
        testContext.completeNow();
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
