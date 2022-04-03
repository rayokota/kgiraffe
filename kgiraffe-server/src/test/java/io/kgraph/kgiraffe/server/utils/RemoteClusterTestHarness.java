/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kgraph.kgiraffe.server.utils;

import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.server.KGiraffeMain;
import io.kgraph.kgiraffe.server.notifier.VertxNotifier;
import io.kgraph.kgiraffe.utils.ClusterTestHarness;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.WebSocketConnectOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
@Tag("IntegrationTest")
public abstract class RemoteClusterTestHarness extends ClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterTestHarness.class);

    protected Integer serverPort;

    protected WebClient webClient;
    protected HttpClient wsClient;
    protected WebSocketConnectOptions wsOptions;

    protected Properties props;
    protected KGiraffeMain verticle;
    protected KGiraffeEngine engine;

    public RemoteClusterTestHarness() {
        super();
    }

    public RemoteClusterTestHarness(int numBrokers) {
        super(numBrokers);
    }

    public KGiraffeMain getVerticle() {
        return verticle;
    }

    public KGiraffeEngine getEngine() {
        return engine;
    }

    @BeforeEach
    public void setUp(Vertx vertx, VertxTestContext testContext) throws Exception {
        super.setUp();

        serverPort = choosePort();
        setUpClient(vertx);
        setUpServer(vertx);

        vertx.deployVerticle(getVerticle(), testContext.succeedingThenComplete());
    }

    private void setUpClient(Vertx vertx) {
        HttpClient client = vertx.createHttpClient(new HttpClientOptions()
            .setDefaultPort(serverPort)
            .setDefaultHost("localhost"));
        webClient = WebClient.wrap(client);

        wsClient = vertx.createHttpClient();
        wsOptions = new WebSocketConnectOptions().setPort(serverPort)
            .addSubProtocol("graphql-ws").setURI("/graphql");
    }

    private void setUpServer(Vertx vertx) {
        try {
            props = new Properties();
            injectKGiraffeProperties(props);

            KGiraffeConfig config = new KGiraffeConfig(props);
            verticle = new KGiraffeMain(config);

            engine = KGiraffeEngine.getInstance();
            engine.configure(config);
            engine.init(new VertxNotifier(vertx.eventBus()));
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }

    protected void injectKGiraffeProperties(Properties props) {
        props.put(KGiraffeConfig.LISTENER_CONFIG, "http://0.0.0.0:" + serverPort);
        props.put(KGiraffeConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KGiraffeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test");
        props.put(KGiraffeConfig.TOPICS_CONFIG, "t1");
    }

    /**
     * Choose a number of random available ports
     */
    public static int[] choosePorts(int count) {
        try {
            ServerSocket[] sockets = new ServerSocket[count];
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0, 0, InetAddress.getByName("0.0.0.0"));
                ports[i] = sockets[i].getLocalPort();
            }
            for (int i = 0; i < count; i++) {
                sockets[i].close();
            }
            return ports;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Choose an available port
     */
    public static int choosePort() {
        return choosePorts(1)[0];
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            KGiraffeEngine.closeInstance();
        } catch (Exception e) {
            LOG.warn("Exception during tearDown", e);
        }
        super.tearDown();
    }
}
