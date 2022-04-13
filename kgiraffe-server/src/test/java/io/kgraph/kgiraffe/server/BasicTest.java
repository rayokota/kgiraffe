package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
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

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicTest extends RemoteClusterTestHarness {

    private WebClient webClient;

    @BeforeEach
    public void setUp(Vertx vertx, VertxTestContext testContext) throws Exception {
        super.setUp(vertx, testContext);
        HttpClient client = vertx.createHttpClient(new HttpClientOptions()
            .setDefaultPort(serverPort)
            .setDefaultHost("localhost"));
        webClient = WebClient.wrap(client);
        testContext.completeNow();
    }

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

        RxHelper.deployVerticle(vertx, getVerticle())
            .flatMap(
                id -> webClient.post("/graphql")
                    .expect(ResponsePredicate.SC_OK)
                    .expect(ResponsePredicate.JSON)
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(request)
            )
            .blockingSubscribe(
                httpResponse -> {
                    Map<String, Object> executionResult = httpResponse.body().getMap();
                    Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                    testContext.verify(() -> {
                        assertThat(result.get("__schema")).isNotNull();
                    });
                },
                testContext::failNow
            );

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
        testContext.completeNow();
    }
}
