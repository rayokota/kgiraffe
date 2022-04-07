package io.kgraph.kgiraffe.schema.timeout;

import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationFieldFetchParameters;
import graphql.schema.DataFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxQueryDurationInstrumentation extends SimpleInstrumentation {

    private static final Logger LOG = LoggerFactory.getLogger(MaxQueryDurationInstrumentation.class);

    private long maxDuration;

    public MaxQueryDurationInstrumentation(long maxDuration) {
        LOG.info("Loaded max query duration instrumentation.");
        this.maxDuration = maxDuration;
    }

    @Override
    public InstrumentationState createState() {
        return new MaxQueryInstrumentationState();
    }

    @Override
    public DataFetcher<?> instrumentDataFetcher(DataFetcher<?> dataFetcher,
                                                InstrumentationFieldFetchParameters parameters) {

        MaxQueryInstrumentationState state = parameters.getInstrumentationState();

        if (state.getTime() > maxDuration) {
            LOG.warn("Max duration: {}, current time: {}", maxDuration, state.getTime());
            return new TimeoutDataFetcher<>(maxDuration);
        }
        return super.instrumentDataFetcher(dataFetcher, parameters);
    }
}
