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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "kgiraffe", mixinStandardHelpOptions = true, version = "kgiraffe 0.1",
    description = "Schema-driven GraphQL for Apache Kafka.", sortOptions = false)
public class KGiraffeMain extends AbstractVerticle implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeMain.class);

    private KGiraffeConfig config;
    private URI listener;

    @Option(names = {"-t", "--topic"},
        description = "Topic(s) to consume from and produce to", paramLabel = "<topic>")
    private List<String> topics;

    @Option(names = {"-p", "--partition"},
        description = "Partition(s)", paramLabel = "<partition>")
    private List<Integer> partitions;

    @Option(names = {"-b", "--bootstrap-server"},
        description = "Bootstrap broker(s) (host:[port])", paramLabel = "<broker>")
    private List<String> bootstrapBrokers;

    @Option(names = {"-m", "--metadata-timeout"},
        description = "Metadata (et.al.) request timeout", paramLabel = "<ms>")
    private Integer initTimeout;

    @Option(names = {"-F", "--file"},
        description = "Read configuration properties from file", paramLabel = "<config-file>")
    private File configFile;

    @Option(names = {"-o", "--offset"},
        description = "Offset to start consuming from:\n"
            + "  beginning | end |\n"
            + "  <value>  (absolute offset) |\n"
            + "  -<value> (relative offset from end)\n"
            + "  @<value> (timestamp in ms to start at)\n"
            + "  Default: beginning")
    private String offset;

    @Option(names = {"-k", "--key-serde"},
        description = "(De)serialize keys using <serde>", paramLabel = "<topic=serde>")
    private Map<String, String> keySerdes;

    @Option(names = {"-v", "--value-serde"},
        description = "(De)serialize values using <serde>\n"
            + "Available serdes:\n"
            + "  short | int | long | float |\n"
            + "  double | string | binary |\n"
            + "  latest (use latest version in SR) |\n"
            + "  <id>   (use schema id from SR)\n"
            + "  Default: latest",
        paramLabel = "<topic=serde>")
    private Map<String, String> valueSerdes;

    @Option(names = {"-r", "--schema-registry-url"},
        description = "SR (Schema Registry) URL", paramLabel = "<url>")
    private String schemaRegistryUrl;

    @Option(names = {"-X", "--property"},
        description = "Set kgiraffe configuration property.", paramLabel = "<prop=val>")
    private Map<String, String> properties;

    public KGiraffeMain() {
    }

    public KGiraffeMain(KGiraffeConfig config) {
        this.config = config;
    }

    @Override
    public Integer call() throws Exception {
        System.out.println(bootstrapBrokers);
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

        Thread t = new Thread(()-> {
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
        CommandLine commandLine = new CommandLine(new KGiraffeMain());
        commandLine.setUsageHelpLongOptionsMaxWidth(30);
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
}
