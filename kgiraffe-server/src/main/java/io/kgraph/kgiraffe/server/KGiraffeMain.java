package io.kgraph.kgiraffe.server;

import graphql.GraphQL;
import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.reactivex.rxjava3.core.Single;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.http.HttpServer;
import io.vertx.rxjava3.ext.web.handler.BodyHandler;
import io.vertx.rxjava3.ext.web.handler.StaticHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.rxjava3.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;

@Command(name = "kgiraffe", mixinStandardHelpOptions = true, version = "kgiraffe 0.1",
    description = "Schema-driven GraphQL for Apache Kafka.")
public class KGiraffeMain extends AbstractVerticle implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeMain.class);

    private KGiraffeConfig config;
    private URI listener;

    @Option(names = {"-F", "--file"}, description = "The configuration file.")
    private File configFile;

    public KGiraffeMain() {
    }

    public KGiraffeMain(KGiraffeConfig config) {
        this.config = config;
    }

    @Override
    public Integer call() throws Exception {
        if (configFile != null) {
            this.config = new KGiraffeConfig(configFile);
        }
        // TODO use config
        this.listener = new URI("http://0.0.0.0:8765");

        KGiraffeEngine engine = KGiraffeEngine.getInstance();
        engine.configure(config);
        Vertx vertx = Vertx.vertx();
        engine.init(vertx.eventBus());
        vertx.deployVerticle(this).toFuture().get();

        var t = new Thread(()-> {
            try {
                System.in.read();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();
        t.join();
        return 0;
    }

    @Override
    public void start() throws Exception {
        try {
            KGiraffeEngine engine = KGiraffeEngine.getInstance();

            Router router = Router.router(vertx);
            GraphQL graphQL = engine.getGraphQL();
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
                .handler(ApolloWSHandler.create(graphQL, apolloWSOptions))
                .handler(GraphQLHandler.create(graphQL, graphQLOptions));

            router.route("/kgiraffe/*")
                .handler(StaticHandler.create("kgiraffe"));

            /*
            GraphiQLHandlerOptions graphiQLOptions = new GraphiQLHandlerOptions()
                .setEnabled(true);
            router.route("/graphiql/*")
                .handler(GraphiQLHandler.create(graphiQLOptions));
            */

            // Create the HTTP server
            HttpServerOptions httpServerOptions = new HttpServerOptions()
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
                    LOG.info("Server started, listening on {}", listener.getPort());
                    LOG.info("GraphQL:     http://localhost:{}/graphql", listener.getPort());
                    LOG.info("GraphQL-WS:  ws://localhost:{}/graphql", listener.getPort());
                    LOG.info("GraphiQL:    http://localhost:{}/kgiraffe", listener.getPort());
                    LOG.info("      /)/)  ");
                    LOG.info("     ( ..\\  ");
                    LOG.info("     /'-._) ");
                    LOG.info("    /#/     ");
                    LOG.info("   /#/      ");
                    LOG.info("  /#/       ");
                    LOG.info("KGiraffe is at your service...");
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

    public static void main(String[] args) {
        int exitCode = new CommandLine(new KGiraffeMain()).execute(args);
        System.exit(exitCode);
    }
}
