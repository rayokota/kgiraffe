package io.kgraph.kgraphql.schema;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.analysis.MaxQueryComplexityInstrumentation;
import graphql.analysis.MaxQueryDepthInstrumentation;
import graphql.execution.instrumentation.ChainedInstrumentation;
import graphql.execution.instrumentation.Instrumentation;
import graphql.schema.GraphQLSchema;
import io.kgraph.kgraphql.KafkaGraphQLConfig;
import io.kgraph.kgraphql.schema.timeout.MaxQueryDurationInstrumentation;

import java.util.Arrays;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class GraphQLExecutor {

    private volatile GraphQL graphQL;
    private final GraphQLSchemaBuilder graphQLSchemaBuilder;
    private final int maxComplexity;
    private final int maxDepth;
    private final int timeoutMs;

    public GraphQLExecutor(KafkaGraphQLConfig config,
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
                    this.graphQL = GraphQL
                        .newGraphQL(graphQLSchema)
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
