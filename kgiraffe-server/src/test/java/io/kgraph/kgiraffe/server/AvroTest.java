package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.KGiraffeConfig;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AvroTest extends RemoteClusterTestHarness {

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

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testBasic(Vertx vertx, VertxTestContext testContext) throws Exception {
        RxHelper.deployVerticle(vertx, getVerticle()).blockingGet();

        JsonObject request = new JsonObject().put("query",
            "{\n" + "  __schema {\n"
                + "    queryType {\n"
                + "      name\n"
                + "    }\n"
                + "  }\n"
                + "}");

        Single<HttpResponse<JsonObject>> response = webClient.post("/graphql")
            .expect(ResponsePredicate.SC_OK)
            .expect(ResponsePredicate.JSON)
            .as(BodyCodec.jsonObject())
            .sendJsonObject(request);

        response.blockingSubscribe(
            json -> {
                System.out.println("success = " + json.body());
            },
            failure -> {
                System.out.println("response = " + failure);
                System.out.println("response = " + failure);
                System.out.println("response = " + failure);
                System.out.println("response = " + failure);
                throw failure;
            });

        testContext.completeNow();

        //assertThat(getResp2.getKvs()).hasSize(1);
        //assertThat(getResp2.getKvs().get(0).getValue().toString(UTF_8)).isEqualTo(oneTwoThree.toString(UTF_8));
    }
    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'");
    }
}
