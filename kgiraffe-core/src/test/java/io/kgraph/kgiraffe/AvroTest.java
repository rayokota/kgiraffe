package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import io.kgraph.kgiraffe.utils.CapturingSubscriber;
import io.kgraph.kgiraffe.utils.LocalClusterTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);
        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'");
    }
}
