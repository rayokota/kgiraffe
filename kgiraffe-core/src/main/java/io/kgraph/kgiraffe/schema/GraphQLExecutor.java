package io.kgraph.kgiraffe.schema;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.analysis.MaxQueryComplexityInstrumentation;
import graphql.analysis.MaxQueryDepthInstrumentation;
import graphql.execution.AsyncExecutionStrategy;
import graphql.execution.AsyncSerialExecutionStrategy;
import graphql.execution.SubscriptionExecutionStrategy;
import graphql.execution.instrumentation.ChainedInstrumentation;
import graphql.execution.instrumentation.Instrumentation;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import io.kgraph.kgiraffe.KGiraffeConfig;
import io.kgraph.kgiraffe.schema.timeout.MaxQueryDurationInstrumentation;
import io.kgraph.kgiraffe.schema.util.DefaultDataFetcherExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class GraphQLExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(GraphQLExecutor.class);

    private volatile GraphQL graphQL;
    private final GraphQLSchemaBuilder graphQLSchemaBuilder;
    private final int maxComplexity;
    private final int maxDepth;
    private final int timeoutMs;

    public GraphQLExecutor(KGiraffeConfig config,
                           GraphQLSchemaBuilder builder) {
        this.graphQLSchemaBuilder = builder;
        this.maxComplexity = config.getGraphQLMaxComplexity();
        this.maxDepth = config.getGraphQLMaxDepth();
        this.timeoutMs = config.getGraphQLTimeoutMs();
    }

    public GraphQL getGraphQL() {
        if (graphQL == null) {
            synchronized (this) {
                if (graphQL == null) {
                    GraphQLSchema graphQLSchema = graphQLSchemaBuilder.getGraphQLSchema();
                    String sdl = new SchemaPrinter().print(graphQLSchema);
                    LOG.debug("SDL: " + sdl);
                    DefaultDataFetcherExceptionHandler handler =
                        new DefaultDataFetcherExceptionHandler();
                    this.graphQL = GraphQL
                        .newGraphQL(graphQLSchema)
                        .queryExecutionStrategy(new AsyncExecutionStrategy(handler))
                        .mutationExecutionStrategy(new AsyncSerialExecutionStrategy(handler))
                        .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy(handler))
                        .instrumentation(getInstrumentation())
                        .build();
                }
            }
        }
        return graphQL;
    }

    private Instrumentation getInstrumentation() {
        return new ChainedInstrumentation(Arrays.asList(
            new MaxQueryDepthInstrumentation(maxDepth),
            new MaxQueryDurationInstrumentation(timeoutMs),
            new MaxQueryComplexityInstrumentation(maxComplexity)
        ));
    }

    public ExecutionResult execute(String query) {
        return execute(query, null);
    }

    public ExecutionResult execute(String query, Map<String, Object> arguments) {
        ExecutionInput.Builder builder = ExecutionInput.newExecutionInput()
            .query(query);
        if (arguments != null) {
            builder = builder.variables(arguments);
        }
        return getGraphQL().execute(builder.build());
    }
}
