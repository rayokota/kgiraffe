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

package io.kgraph.kgiraffe.utils;

import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.notifier.RxBusNotifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Test harness to run against a real, local Kafka cluster. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined.
 */
public abstract class LocalClusterTestHarness extends ClusterTestHarness {

    private static final Logger LOG = LoggerFactory.getLogger(LocalClusterTestHarness.class);

    protected Properties props;

    protected Integer serverPort;
    protected KGiraffeEngine engine;

    public LocalClusterTestHarness() {
        super();
    }

    public LocalClusterTestHarness(int numBrokers) {
        super(numBrokers);
    }

    public KGiraffeEngine getEngine() throws Exception {
        return engine;
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        setUpServer();
    }

    private void setUpServer() {
        try {
            props = new Properties();
            injectKGiraffeProperties(props);

            KGiraffeConfig config = new KGiraffeConfig(props);

            engine = KGiraffeEngine.getInstance();
            engine.configure(config);
            engine.init(new RxBusNotifier());
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
