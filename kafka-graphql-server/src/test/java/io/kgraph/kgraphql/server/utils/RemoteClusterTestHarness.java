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

package io.kgraph.kgraphql.server.utils;

import com.google.common.io.Files;
import io.kgraph.kgraphql.KafkaGraphQLConfig;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.kgraph.kgraphql.server.KafkaGraphQLMain;
import io.kgraph.kgraphql.utils.ClusterTestHarness;
import io.vertx.rxjava3.core.Vertx;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

import static org.mockito.Mockito.mock;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class RemoteClusterTestHarness extends ClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteClusterTestHarness.class);

    protected Properties props;
    protected String endpoints;

    protected File tempDir;
    protected Integer serverPort;

    public RemoteClusterTestHarness() {
        super();
    }

    public RemoteClusterTestHarness(int numBrokers) {
        super(numBrokers);
    }

    public KafkaGraphQLMain createKafkaGraphQL() throws Exception {
        props = new Properties();
        endpoints = "http://127.0.0.1:" + choosePort();
        return new KafkaGraphQLMain(new KafkaGraphQLConfig(props));
    }

    @BeforeEach
    public void setUp(Vertx vertx) throws Exception {
        super.setUp();
        if (tempDir == null) {
            tempDir = Files.createTempDir();
        }
        setUpServer(vertx);
    }

    private void setUpServer(Vertx vertx) {
        try {
            serverPort = choosePort();
            injectKafkaGraphQLProperties(props);

            KafkaGraphQLConfig config = new KafkaGraphQLConfig(props);
            KafkaGraphQLEngine engine = KafkaGraphQLEngine.getInstance();
            engine.configure(config);
            engine.init(vertx.eventBus());
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }

    protected void injectKafkaGraphQLProperties(Properties props) {
        props.put(KafkaGraphQLConfig.LISTENERS_CONFIG, "http://0.0.0.0:" + serverPort);
        props.put(KafkaGraphQLConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaGraphQLConfig.KAFKACACHE_DATA_DIR_CONFIG, tempDir.getAbsolutePath());
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
            KafkaGraphQLEngine.closeInstance();
            deleteDirectory(tempDir);
        } catch (Exception e) {
            LOG.warn("Exception during tearDown", e);
        }
        super.tearDown();
    }

    private static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
