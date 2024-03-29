package io.kgraph.kgiraffe.server;

import graphql.GraphQL;
import io.kcache.KafkaCacheConfig;
import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.KGiraffeConfig.ListPropertyParser;
import io.kgraph.kgiraffe.KGiraffeConfig.MapPropertyParser;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.server.notifier.VertxNotifier;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSOptions;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

@Command(name = "kgiraffe", mixinStandardHelpOptions = true,
    versionProvider = KGiraffeMain.ManifestVersionProvider.class,
    description = "A GraphQL Interface for Apache Kafka and Schema Registry.",
    sortOptions = false, sortSynopsis = false)
public class KGiraffeMain extends AbstractVerticle implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeMain.class);

    private static final ListPropertyParser listPropertyParser = new ListPropertyParser();
    private static final MapPropertyParser mapPropertyParser = new MapPropertyParser();

    private KGiraffeConfig config;

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
    private KafkaCacheConfig.Offset offset;

    @Option(names = {"-k", "--key-serde"},
        description = "(De)serialize keys using <serde>", paramLabel = "<topic=serde>")
    private Map<String, KGiraffeConfig.Serde> keySerdes;

    @Option(names = {"-v", "--value-serde"},
        description = "(De)serialize values using <serde>\n"
            + "Available serdes:\n"
            + "  short | int | long | float |\n"
            + "  double | string | binary |\n"
            + "  avro:<schema|@file> |\n"
            + "  json:<schema|@file> |\n"
            + "  proto:<schema|@file> |\n"
            + "  latest (use latest version in SR) |\n"
            + "  <id>   (use schema id from SR)\n"
            + "  Default for key:   binary\n"
            + "  Default for value: latest\n"
            + "The avro/json/proto serde formats can\n"
            + "also be specified with refs, e.g.\n"
            + "  avro:<schema|@file>;refs:<refs|@file>\n"
            + "where refs are schema references\n"
            + "of the form \n"
            + "  [{name=\"<name>\",subject=\"<subject>\",\n"
            + "    version=<version>},..]",
        paramLabel = "<topic=serde>")
    private Map<String, KGiraffeConfig.Serde> valueSerdes;

    @Option(names = {"-r", "--schema-registry-url"},
        description = "SR (Schema Registry) URL", paramLabel = "<url>")
    private String schemaRegistryUrl;

    @Option(names = {"-s", "--stage-schema"},
        description = "Validate and stage the given schema(s).\n"
            + "See avro/json/proto serde formats above.",
        paramLabel = "<serde>")
    private List<KGiraffeConfig.Serde> schemas;

    @Option(names = {"-X", "--property"},
        description = "Set kgiraffe configuration property.", paramLabel = "<prop=val>")
    private Map<String, String> properties;

    public KGiraffeMain() {
    }

    public KGiraffeMain(KGiraffeConfig config) {
        this.config = config;
    }

    public URI getListener() throws URISyntaxException {
        return new URI(config.getString(KGiraffeConfig.LISTENER_CONFIG));
    }

    @Override
    public Integer call() throws Exception {
        if (configFile != null) {
            config = new KGiraffeConfig(configFile);
        }
        config = updateConfig();

        Vertx vertx = Vertx.vertx();

        KGiraffeEngine engine = KGiraffeEngine.getInstance();
        engine.configure(config);
        engine.init(new VertxNotifier(vertx.eventBus()));

        vertx.deployVerticle(this);

        Thread t = new Thread(() -> {
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
    public void start(Promise<Void> startPromise) {
        KGiraffeEngine engine = KGiraffeEngine.getInstance();
        try {
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
            URI listener = getListener();
            HttpServerOptions httpServerOptions = new HttpServerOptions()
                .addWebSocketSubProtocol("graphql-ws")
                .setTcpKeepAlive(true);
            vertx.createHttpServer(httpServerOptions)
                // Handle every request using the router
                .requestHandler(router)
                .exceptionHandler(it -> LOG.error("Server error", it))
                // Start listening
                .listen(listener.getPort(), listener.getHost(), ar -> {
                    if (ar.succeeded()) {
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
                        startPromise.complete();
                    } else {
                        LOG.info("Could not start server: ", ar.cause());
                        startPromise.fail(ar.cause());
                        LOG.error("Server died unexpectedly: ", ar.cause());
                        System.exit(1);
                    }
                });
        } catch (Exception e) {
            LOG.info("Could not start server: {}", e.getLocalizedMessage());
            startPromise.fail(e);
            LOG.error("Server died unexpectedly: ", e);
            System.exit(1);
        }
    }

    private KGiraffeConfig updateConfig() {
        Map<String, String> props = new HashMap<>();
        if (config != null) {
            props.putAll(config.originalsStrings());
        }
        if (topics != null) {
            props.put(KGiraffeConfig.TOPICS_CONFIG, String.join(",", topics));
        }
        if (partitions != null) {
            props.put(KGiraffeConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, partitions.stream()
                .map(Object::toString)
                .collect(Collectors.joining(",")));
        }
        if (bootstrapBrokers != null) {
            props.put(KGiraffeConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapBrokers));
        }
        if (initTimeout != null) {
            props.put(KGiraffeConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, String.valueOf(initTimeout));
        }
        if (offset != null) {
            props.put(KGiraffeConfig.KAFKACACHE_TOPIC_PARTITIONS_OFFSET_CONFIG, offset.toString());
        }
        if (keySerdes != null) {
            props.put(KGiraffeConfig.KEY_SERDES_CONFIG,
                mapPropertyParser.asString(keySerdes.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()))
                ));
        }
        if (valueSerdes != null) {
            props.put(KGiraffeConfig.VALUE_SERDES_CONFIG,
                mapPropertyParser.asString(valueSerdes.entrySet().stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()))
                ));
        }
        if (schemaRegistryUrl != null) {
            props.put(KGiraffeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        }
        if (schemas != null) {
            props.put(KGiraffeConfig.STAGE_SCHEMAS_CONFIG,
                listPropertyParser.asString(schemas.stream()
                    .map(KGiraffeConfig.Serde::toString)
                    .collect(Collectors.toList())));
        }
        if (properties != null) {
            props.putAll(properties);
        }
        return new KGiraffeConfig(props);
    }

    static class OffsetConverter implements CommandLine.ITypeConverter<KafkaCacheConfig.Offset> {
        @Override
        public KafkaCacheConfig.Offset convert(String value) {
            try {
                return new KafkaCacheConfig.Offset(value);
            } catch (ConfigException e) {
                throw new CommandLine.TypeConversionException("expected one of [beginning, end, "
                    + "<value>, -<value>, @<value>] but was '" + value + "'");
            }
        }
    }

    static class SerdeConverter implements CommandLine.ITypeConverter<KGiraffeConfig.Serde> {
        @Override
        public KGiraffeConfig.Serde convert(String value) {
            try {
                return new KGiraffeConfig.Serde(value);
            } catch (ConfigException e) {
                throw new CommandLine.TypeConversionException("expected one of [short, int, "
                    + "long, float, double, string, binary, latest, <id>] but was '"
                    + value + "'");
            }
        }
    }

    static class ManifestVersionProvider implements CommandLine.IVersionProvider {
        public String[] getVersion() throws Exception {
            Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                try {
                    Manifest manifest = new Manifest(url.openStream());
                    if (isApplicableManifest(manifest)) {
                        Attributes attr = manifest.getMainAttributes();
                        return new String[]{
                            "kgiraffe - A GraphQL Interface for Apache Kafka and Schema Registry",
                            "https://github.com/rayokota/kgiraffe",
                            "Copyright (c) 2022, Robert Yokota",
                            "Version " + get(attr, "Implementation-Version")
                        };
                    }
                } catch (IOException ex) {
                    return new String[]{"Unable to read from " + url + ": " + ex};
                }
            }
            return new String[0];
        }

        private boolean isApplicableManifest(Manifest manifest) {
            Attributes attributes = manifest.getMainAttributes();
            return "kgiraffe-server".equals(get(attributes, "Implementation-Title"));
        }

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new KGiraffeMain());
        commandLine.registerConverter(KafkaCacheConfig.Offset.class, new OffsetConverter());
        commandLine.registerConverter(KGiraffeConfig.Serde.class, new SerdeConverter());
        commandLine.setUsageHelpLongOptionsMaxWidth(30);
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
}
