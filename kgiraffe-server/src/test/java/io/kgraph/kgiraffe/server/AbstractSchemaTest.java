package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.ApolloWSMessageType;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kgraph.kgiraffe.server.utils.ChainRequestHelper.requestWithFuture;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractSchemaTest extends RemoteClusterTestHarness {

    @Test
    @SuppressWarnings("unchecked")
    public void testSimple(Vertx vertx, VertxTestContext testContext) throws Exception {
        JsonObject mutation = new JsonObject().put("query", "mutation {\n" +
            "  t1(value: { f1: \"hello\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}");

        JsonObject query = new JsonObject().put("query", "{\n" +
            "  t1 (where: {value: {f1: {_eq: \"hello\"}}}) {\n" +
            "    value {\n" +
            "      f1 \n" +
            "    }\n" +
            "    topic\n" +
            "    offset\n" +
            "    partition\n" +
            "    ts\n" +
            "  }\n" +
            "}");

        JsonObject subscription = new JsonObject().put("query", "subscription {\n" +
            "  t1 {\n" +
            "    value {\n" +
            "    \tf1\n" +
            "    }\n" +
            "  }\n" +
            "}");

        JsonObject wsStart = new JsonObject()
            .put("id", "1")
            .put("type", ApolloWSMessageType.START.getText())
            .put("payload", subscription);

        JsonObject mutation2 = new JsonObject().put("query", "mutation {\n" +
            "  t1(value: { f1: \"world\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}");

        CompletableFuture<JsonObject> eventFuture = new CompletableFuture<>();
        eventFuture.whenComplete((r, t) -> wsClient.close());

        requestWithFuture(webClient, "/graphql", mutation)
            .thenCompose(response -> {
                Map<String, Object> executionResult = response.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
                Map<String, Object> value = (Map<String, Object>) t1.get("value");
                String f1 = (String) value.get("f1");
                testContext.verify(() -> assertThat(f1).isEqualTo("hello"));

                return requestWithFuture(webClient, "/graphql", query);
            })
            .thenCompose(response -> {
                Map<String, Object> executionResult = response.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                List<Map<String, Object>> t1s = (List<Map<String, Object>>) result.get("t1");
                Map<String, Object> t1 = t1s.get(0);
                Map<String, Object> value = (Map<String, Object>) t1.get("value");
                String f1 = (String) value.get("f1");
                testContext.verify(() -> assertThat(f1).isEqualTo("hello"));

                return websocketRequest(wsClient, wsOptions, wsStart, eventFuture);
            })
            .thenCompose(response ->
                requestWithFuture(webClient, "/graphql", mutation2)
            )
            .whenComplete((response, t) -> {
                Map<String, Object> executionResult = response.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
                Map<String, Object> value = (Map<String, Object>) t1.get("value");
                String f1 = (String) value.get("f1");
                testContext.verify(() -> assertThat(f1).isEqualTo("world"));
            });

        JsonObject event = eventFuture.get(60, TimeUnit.SECONDS);
        Map<String, Object> executionResult = (Map<String, Object>) event.getMap().get("payload");
        Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
        Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
        Map<String, Object> value = (Map<String, Object>) t1.get("value");
        String f1 = (String) value.get("f1");
        testContext.verify(() -> {
            assertThat(f1).isEqualTo("world");
            testContext.completeNow();
        });
    }

    // Adapted from the quarkus vertx-graphql integration test
    private CompletableFuture<Void> websocketRequest(HttpClient wsClient,
                                                     WebSocketConnectOptions wsOptions, JsonObject graphql,
                                                     CompletableFuture<JsonObject> event) {

        /*
         * Protocol:
         * --> connection_init -->
         * <-- connection_ack <--
         * <-- ka (keep-alive) <--
         * -----> start ------>
         * <----- data <-------
         */

        CompletableFuture<Void> wsFuture = new CompletableFuture<>();
        JsonObject init = new JsonObject().put("type", ApolloWSMessageType.CONNECTION_INIT.getText());
        wsClient.webSocket(wsOptions, ws -> {
            AtomicReference<ApolloWSMessageType> lastReceivedType = new AtomicReference<>();
            if (ws.succeeded()) {
                WebSocket webSocket = ws.result();
                webSocket.handler(message -> {
                    JsonObject json = message.toJsonObject();
                    ApolloWSMessageType type = ApolloWSMessageType.from(json.getString("type"));
                    switch (type) {
                        case CONNECTION_ACK:
                            webSocket.write(graphql.toBuffer(), ar -> {
                                if (ar.succeeded()) {
                                    wsFuture.complete(null);
                                } else {
                                    wsFuture.completeExceptionally(ar.cause());
                                }
                            });
                            break;
                        case CONNECTION_KEEP_ALIVE:
                            break;
                        case DATA:
                            event.complete(json);
                            break;
                        default:
                            break;
                    }
                    lastReceivedType.set(type);
                });
                webSocket.write(init.toBuffer());
            } else {
                wsFuture.completeExceptionally(ws.cause());
            }
        });

        return wsFuture;
    }
}
