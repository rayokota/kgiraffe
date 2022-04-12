package io.kgraph.kgiraffe.server;

import io.kgraph.kgiraffe.server.utils.RemoteClusterTestHarness;
import io.netty.channel.EventLoopGroup;
import io.reactivex.rxjava3.core.Single;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.TimeoutStream;
import io.vertx.core.Verticle;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.dns.DnsClientOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.spi.VerticleFactory;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Charsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

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
}
