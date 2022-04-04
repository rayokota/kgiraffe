package io.kgraph.kgraphql.server;

import graphql.GraphQL;
import io.kgraph.kgraphql.KafkaGraphQLConfig;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class KafkaGraphQLMain extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaGraphQLMain.class);

    private final KafkaGraphQLConfig config;
    private final URI listener;

    public KafkaGraphQLMain(KafkaGraphQLConfig config)
        throws URISyntaxException {
        this.config = config;
        this.listener = new URI("http://0.0.0.0:8765");
        /*
        this.listener = elector.getListeners().isEmpty()
            ? new URI("http://0.0.0.0:2379")
            : elector.getListeners().get(0);

         */
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        KafkaGraphQLEngine engine = KafkaGraphQLEngine.getInstance();

        Router router = Router.router(vertx);
        GraphQL graphQL = engine.getGraphQL();
        router.route().handler(LoggerHandler.create());
        router.route().handler(BodyHandler.create());
        router.route("/graphql").handler(GraphQLHandler.create(engine.getGraphQL()));

        GraphiQLHandlerOptions options = new GraphiQLHandlerOptions().setEnabled(true);
        router.route("/graphiql/*").handler(GraphiQLHandler.create(options));

        // Create the HTTP server
        vertx.createHttpServer()
            // Handle every request using the router
            .requestHandler(router)
            // Start listening
            .listen(listener.getPort(), ar -> {
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
