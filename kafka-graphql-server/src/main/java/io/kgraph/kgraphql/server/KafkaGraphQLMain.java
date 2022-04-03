package io.kgraph.kgraphql.server;

import io.kgraph.kgraphql.KafkaGraphQLConfig;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class KafkaGraphQLMain extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaGraphQLMain.class);

    private final KafkaGraphQLConfig config;
    private final URI listener = null;

    public KafkaGraphQLMain(KafkaGraphQLConfig config)
        throws URISyntaxException {
        this.config = config;
        /*
        this.listener = elector.getListeners().isEmpty()
            ? new URI("http://0.0.0.0:2379")
            : elector.getListeners().get(0);

         */
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        /*
        VertxServerBuilder serverBuilder = VertxServerBuilder
            .forAddress(vertx,
                this.context.config().getString("listen-address", listener.getHost()),
                this.context.config().getInteger("listen-port", listener.getPort()));

        List<ServerServiceDefinition> services = Arrays.asList(
            new AuthService(elector).bindService(),
            new KVService(elector).bindService(),
            new LeaseService(elector).bindService()
        );


        NettyServerBuilder nettyBuilder = serverBuilder.nettyBuilder()
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(5, TimeUnit.SECONDS)
            // may help with "java.net.BindException: address already in use"
            // see https://issues.apache.org/jira/browse/RATIS-606
            .withChildOption(ChannelOption.SO_REUSEADDR, true)
            .withChildOption(ChannelOption.TCP_NODELAY, true)
            .addService(new ClusterService(elector))
            .addService(new MaintenanceService(elector))
            .addService(new WatchService(elector))  // WatchService can go to any node
            .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy, services))
            .intercept(new AuthServerInterceptor());

        if (isTls()) {
            nettyBuilder.sslContext(new SslFactory(config, true).sslContext());
        }

        VertxServer server = serverBuilder.build();

        server.start(ar -> {
            if (ar.succeeded()) {
                LOG.info("Server started, listening on " + listener.getPort());
                LOG.info("Kafka GraphQL is at your service...");
                startPromise.complete();
            } else {
                LOG.info("Could not start server " + ar.cause().getLocalizedMessage());
                startPromise.fail(ar.cause());
                LOG.error("Server died unexpectedly: ", ar.cause());
                System.exit(1);
            }
        });
         */
    }

    private boolean isTls() {
        return listener.getScheme().equalsIgnoreCase("https");
    }

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                LOG.error("Properties file is required to start");
                System.exit(1);
            }
            final KafkaGraphQLConfig config = new KafkaGraphQLConfig(args[0]);
            KafkaGraphQLEngine engine = KafkaGraphQLEngine.getInstance();
            engine.configure(config);
            Vertx vertx = Vertx.vertx();
            engine.init();
            vertx.deployVerticle(new KafkaGraphQLMain(config));
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }
}
