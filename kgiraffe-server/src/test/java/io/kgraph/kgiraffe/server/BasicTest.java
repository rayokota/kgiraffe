package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static io.kgraph.kgiraffe.server.utils.ChainRequestHelper.requestWithFuture;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicTest extends RemoteClusterTestHarness {

    @Test
    @SuppressWarnings("unchecked")
    public void testSchemaQuery(Vertx vertx, VertxTestContext testContext) throws Exception {
        JsonObject request = new JsonObject().put("query", "{\n"
            + "  __schema {\n"
            + "    queryType {\n"
            + "      name\n"
            + "    }\n"
            + "  }\n"
            + "}");

        requestWithFuture(webClient, "/graphql", request)
            .thenCompose(response -> {
                Map<String, Object> executionResult = response.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                testContext.verify(() -> {
                    assertThat(result.get("__schema")).isNotNull();
                });

                return requestWithFuture(webClient, "/graphql", request);
            })
            .whenComplete((response, t) -> {
                if (t != null) {
                    testContext.failNow(t);
                }
                Map<String, Object> executionResult = response.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                testContext.verify(() -> {
                    assertThat(result.get("__schema")).isNotNull();
                });

                testContext.completeNow();
            });
    }

}
