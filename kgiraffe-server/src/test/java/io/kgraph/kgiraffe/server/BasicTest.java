package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.WebClient;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.kgraph.kgiraffe.server.utils.ChainRequestHelper.requestWithFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

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

            /*
            Future<HttpResponse<JsonObject>> future =webClient.post("/graphql")
                .expect(ResponsePredicate.SC_OK)
                .expect(ResponsePredicate.JSON)
                .as(BodyCodec.jsonObject())
                .sendJsonObject(request);

            future.
                .compose(response -> {
                    Map<String, Object> executionResult = response.body().getMap();
                    Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                    testContext.verify(() -> {
                        assertThat(result.get("__schema")).isNotNull();
                    });

                    return webClient.post("/graphql")
                        .expect(ResponsePredicate.SC_OK)
                        .expect(ResponsePredicate.JSON)
                        .as(BodyCodec.jsonObject())
                        .sendJsonObject(request);
                    , ar -> {
                            if (ar.succeeded()) {
                                HttpResponse<JsonObject> resp = ar.result();
                                Map<String, Object> execResult = response.body().getMap();
                                Map<String, Object> data = (Map<String, Object>) executionResult.get("data");
                                testContext.verify(() -> {
                                    assertThat(data.get("__schema")).isNotNull();
                                    testContext.completeNow();
                                });
                            } else {
                                testContext.failNow(ar.cause());
                            }
                        });
                });



        /*
        webClient.post("/graphql")
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(request)

            .blockingSubscribe(
            httpResponse -> {
                Map<String, Object> map = httpResponse.body().getMap();
                Map<String, Object> executionResult = (Map<String, Object>) map.get("data");
                testContext.verify(() -> {
                    assertThat(executionResult.get("__schema")).isNotNull();
                });
            },
            testContext::failNow
        );

         */
    }

}
