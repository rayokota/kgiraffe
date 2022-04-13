package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.ApolloWSMessageType;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rxjava3.core.RxHelper;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractSchemaTest extends RemoteClusterTestHarness {

    private HttpClient client;
    private WebClient webClient;

    @BeforeEach
    public void setUp(Vertx vertx, VertxTestContext testContext) throws Exception {
        super.setUp(vertx, testContext);
        client = vertx.createHttpClient(new HttpClientOptions()
            .setDefaultPort(serverPort)
            .setDefaultHost("localhost"));
        webClient = WebClient.wrap(client);
        testContext.completeNow();
    }

    /*
    public void start() {
        HttpClient httpClient = vertx.createHttpClient(new HttpClientOptions().setDefaultPort(8080));
        httpClient.webSocket("/graphql", websocketRes -> {
            if (websocketRes.succeeded()) {
                WebSocket webSocket = websocketRes.result();

                webSocket.handler(message -> {
                    System.out.println(message.toJsonObject().encodePrettily());
                });

                JsonObject request = new JsonObject()
                    .put("id", "1")
                    .put("type", ApolloWSMessageType.START.getText())
                    .put("payload", new JsonObject()
                        .put("query", "subscription { links { url, postedBy { name } } }"));
                webSocket.write(request.toBuffer());
            } else {
                websocketRes.cause().printStackTrace();
            }
        });
    }

     */

    @Test
    @SuppressWarnings("unchecked")
    public void testSchemaQuery(Vertx vertx, VertxTestContext testContext) throws Exception {
        JsonObject request = new JsonObject().put("query",
            "{\n" + "  __schema {\n"
                + "    queryType {\n"
                + "      name\n"
                + "    }\n"
                + "  }\n"
                + "}");


        RxHelper.deployVerticle(vertx, getVerticle())
            .flatMap(id ->
                webClient.post("/graphql")
                    .expect(ResponsePredicate.SC_OK)
                    .expect(ResponsePredicate.JSON)
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(request)

            )
            .subscribe(
                httpResponse -> {
                    Map<String, Object> map = httpResponse.body().getMap();
                    Map<String, Object> executionResult = (Map<String, Object>) map.get("data");
                    testContext.verify(() -> {
                        assertThat(executionResult.get("__schema")).isNotNull();
                        testContext.completeNow();
                    });
                },
                testContext::failNow);
    }
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

        JsonObject subscription = new JsonObject()
            .put("id", "1")
            .put("type", ApolloWSMessageType.START.getText())
            .put("payload", new JsonObject())
            .put("query", "subscription {\n" +
            "  t1 {\n" +
            "    value {\n" +
            "    \tf1\n" +
            "    }\n" +
            "  }\n" +
            "}");

        JsonObject mutation2 = new JsonObject().put("query", "mutation {\n" +
            "  t1(value: { f1: \"world\"}) {\n" +
            "    value {\n" +
            "      f1\n" +
            "    }\n" +
            "  }\n" +
            "}");

        RxHelper.deployVerticle(vertx, getVerticle())
            .flatMap(id ->
                webClient.post("/graphql")
                    .expect(ResponsePredicate.SC_OK)
                    .expect(ResponsePredicate.JSON)
                    .as(BodyCodec.jsonObject())
                    .sendJsonObject(mutation)

            )
            .blockingSubscribe(
                httpResponse -> {
                    Map<String, Object> executionResult = httpResponse.body().getMap();
                    Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                    Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
                    Map<String, Object> value = (Map<String, Object>) t1.get("value");
                    String f1 = (String) value.get("f1");
                    testContext.verify(() -> {
                        assertThat(f1).isEqualTo("hello");
                    });
                },
                testContext::failNow);

        webClient.post("/graphql")
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(query)

            .blockingSubscribe(
            httpResponse -> {
                Map<String, Object> executionResult = httpResponse.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                List<Map<String, Object>> t1s = (List<Map<String, Object>>) result.get("t1");
                Map<String, Object> t1 = t1s.get(0);
                Map<String, Object> value = (Map<String, Object>) t1.get("value");
                String f1 = (String) value.get("f1");
                testContext.verify(() -> {
                    assertThat(f1).isEqualTo("hello");
                });
            },
            testContext::failNow);

        /*
         * Protocol:
         * --> connection_init -->
         * <-- connection_ack <--
         * <-- ka (keep-alive) <--
         * -----> start ------>
         * <----- data <-------
         */

        JsonObject init = new JsonObject().put("type", ApolloWSMessageType.CONNECTION_INIT.getText());

        client.webSocket("/graphql")
            .blockingSubscribe(webSocket -> {
                AtomicReference<String> lastReceivedType = new AtomicReference<>();
                webSocket.handler(message -> {
                    JsonObject json = message.toJsonObject();
                    ApolloWSMessageType type = ApolloWSMessageType.from(json.getString("type"));
                    System.out.println("**** type " + type);
                    System.out.println("**** type " + type);
                    System.out.println("**** type " + type);
                    System.out.println("**** type " + type);
                    if (ApolloWSMessageType.CONNECTION_ACK.getText().equals(type)) {
                        webSocket.write(new Buffer(subscription.toBuffer()));

                } else if (ApolloWSMessageType.DATA.getText().equals(type)) {
                                System.out.println("*** " +json.encodePrettily());
                                System.out.println("*** " + json.encodePrettily());
                                testContext.completeNow();
                            } else {
                                webSocket.write(new Buffer(subscription.toBuffer()));

                            }
                    });
                webSocket.write(new Buffer(init.toBuffer()));
            });

        Thread.sleep(5000);
        /*
        client.webSocket("/graphql")
            .blockingSubscribe(webSocket -> {
                webSocket.handler(message -> {
                    System.out.println("*** " + message.toJsonObject().encodePrettily());
                    System.out.println("*** " + message.toJsonObject().encodePrettily());
                });
                webSocket.write(new Buffer(subscription.toBuffer()));
                });

         */

        webClient.post("/graphql")
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(mutation2)

            .blockingSubscribe(
            httpResponse -> {
                Map<String, Object> executionResult = httpResponse.body().getMap();
                Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
                Map<String, Object> value = (Map<String, Object>) t1.get("value");
                String f1 = (String) value.get("f1");
                testContext.verify(() -> {
                    assertThat(f1).isEqualTo("world");
                });
            },
            testContext::failNow);

        /*
        webClient.post("/graphql")
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(query)

            .blockingSubscribe(
                httpResponse -> {
                    Map<String, Object> executionResult = httpResponse.body().getMap();
                    Map<String, Object> result = (Map<String, Object>) executionResult.get("data");
                    Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
                    Map<String, Object> value = (Map<String, Object>) t1.get("value");
                    String f1 = (String) value.get("f1");
                    testContext.verify(() -> {
                        assertThat(f1).isEqualTo("hello");
                    });
                },
                testContext::failNow);
        /*
        ExecutionResult executionResult = graphQL.execute(mutation);
        Map<String, Object> result = executionResult.getData();
        Map<String, Object> t1 = (Map<String, Object>) result.get("t1");
        Map<String, Object> value = (Map<String, Object>) t1.get("value");
        String f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");

        executionResult = graphQL.execute(query);
        result = executionResult.getData();
        List<Map<String, Object>> t1s = (List<Map<String, Object>>) result.get("t1");
        t1 = t1s.get(0);
        value = (Map<String, Object>) t1.get("value");
        f1 = (String) value.get("f1");
        assertThat(f1).isEqualTo("hello");

        executionResult = graphQL.execute(subscription);

        Publisher<ExecutionResult> msgStream = executionResult.getData();
        CapturingSubscriber<ExecutionResult> capturingSubscriber =
            new CapturingSubscriber<>();
        msgStream.subscribe(capturingSubscriber);

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

         */
    }
    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'");
    }
}
