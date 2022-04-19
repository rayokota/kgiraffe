package io.kgraph.kgiraffe;

import graphql.ExecutionResult;
import graphql.GraphQL;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);

        String cycle =
            ",'cycle=avro:{\"type\": \"record\",\"name\": \"linked_list\",\"fields\" : "
                + "[{\"name\": \"value\", \"type\": \"int\"},"
                + "{\"name\": \"next\", \"type\": [\"null\", \"linked_list\"],\"default\" : null}]}'";

        String types =
            ",'types=avro:{\n" +
                "  \"namespace\": \"ns\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"MyRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"mynull\",\n" +
                "      \"type\": \"null\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myint\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mynumericlong\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mystringlong\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myfloat\",\n" +
                "      \"type\": \"float\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mydouble\",\n" +
                "      \"type\": \"double\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myboolean\",\n" +
                "      \"type\": \"boolean\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mystring\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mybinary\",\n" +
                "      \"type\": \"bytes\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mysuit\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"enum\",\n" +
                "        \"name\": \"Suit\",\n" +
                "        \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mysuit2\",\n" +
                "      \"type\": \"ns.Suit\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"myarray\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"array\",\n" +
                "        \"items\": \"string\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"mymap\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"map\",\n" +
                "        \"values\": \"string\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}'";

        String serdes = "'t1=avro:{\"type\":\"record\"," +
            "\"name\":\"myrecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}'" +
            ",'t2=avro:{\"type\":\"record\",\"name\":\"myrecord\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}," +
            "{\"name\":\"nested\"," +
            "\"type\":{\"type\": \"record\",\"name\":\"nested\",\"fields\":[{\"name\":\"f2\"," +
            "\"type\":\"string\"}]}}]}'";

        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, serdes + types + cycle);

    }
}
