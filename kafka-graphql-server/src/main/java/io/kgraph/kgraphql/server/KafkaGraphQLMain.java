package io.kgraph.kgraphql.server;

import graphql.GraphQL;
import io.kgraph.kgraphql.KafkaGraphQLConfig;
import io.kgraph.kgraphql.KafkaGraphQLEngine;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.Promise;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import io.vertx.rxjava3.ext.web.handler.LoggerHandler;
import io.vertx.rxjava3.ext.web.handler.StaticHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.handler.CorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;

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
    public void start() throws Exception {
        try {
            KafkaGraphQLEngine engine = KafkaGraphQLEngine.getInstance();

            Router router = Router.router(vertx);
            GraphQL graphQL = engine.getGraphQL();
            router.route().handler(LoggerHandler.create());
            router.route().handler(BodyHandler.create());
            GraphQLHandlerOptions graphQLOptions = new GraphQLHandlerOptions()
                .setRequestBatchingEnabled(true)
                .setRequestMultipartEnabled(true);
            ApolloWSOptions apolloWSOptions = new ApolloWSOptions()
                // GraphQL Playground has a hard-coded timeout of 20000ms
                // See https://github.com/graphql/graphql-playground/issues/1247
                // Also, GraphiQL in vertx-web does not support Apollo WS
                // See https://github.com/vert-x3/vertx-web/issues/1415
                .setKeepAlive(5000L);
            router.route("/graphql")
                //.handler(GraphQLWSHandler.create(graphQL))
                .handler(ApolloWSHandler.create(graphQL, apolloWSOptions))
                .handler(GraphQLHandler.create(graphQL, graphQLOptions));

            router.route("/playground/*")
                .handler(StaticHandler.create("playground"));

            GraphiQLHandlerOptions graphiQLOptions = new GraphiQLHandlerOptions()
                .setEnabled(true);
            router.route("/graphiql/*")
                .handler(GraphiQLHandler.create(graphiQLOptions));

            // Create the HTTP server
            HttpServerOptions httpServerOptions = new HttpServerOptions()
                //.addWebSocketSubProtocol("graphql-transport-ws");
                .addWebSocketSubProtocol("graphql-ws")
                .setTcpKeepAlive(true);
            Single<HttpServer> single = vertx.createHttpServer(httpServerOptions)
                // Handle every request using the router
                .requestHandler(router)
                .exceptionHandler(it -> LOG.error("Server error", it))
                // Start listening
                .rxListen(listener.getPort());

            single.subscribe(
                server -> {
                    LOG.info("Server started, listening on " + listener.getPort());
                    LOG.info("GraphQL:     http://localhost:{}/graphql", listener.getPort());
                    LOG.info("GraphQL-WS:  ws://localhost:{}/graphql", listener.getPort());
                    LOG.info("GraphiQL:    http://localhost:{}/playground", listener.getPort());
                    LOG.info("Kafka GraphQL is at your service...");
                },
                failure -> {
                    LOG.info("Could not start server " + failure);
                    LOG.error("Server died unexpectedly: ", failure);
                    System.exit(1);
                });
        } catch (Exception e) {
            LOG.error("Could not start server", e);
            e.printStackTrace();
            throw e;
        }
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
            engine.init(vertx.eventBus());
            vertx.deployVerticle(new KafkaGraphQLMain(config));
        } catch (Exception e) {
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }
}
