package io.kgraph.kgiraffe;

import java.util.Properties;

public class JsonSchemaTest extends AbstractSchemaTest {

    @Override
    protected void injectKGiraffeProperties(Properties props) {
        super.injectKGiraffeProperties(props);

        String cycle = ",'cycle=json:{\n"
            + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
            + "  \"$id\": \"cycle.json\",\n"
            + "  \"type\": [\"null\", \"object\"],\n"
            + "  \"properties\": {\n"
            + "    \"value\": {\n"
            + "        \"type\": \"integer\"\n"
            + "    },\n"
            + "    \"next\": {\n"
            + "        \"$ref\": \"cycle.json\"\n"
            + "    }    \n"
            + "  }\n"
            + "}'";

        String types = ",'types=json:{\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n" +
                "    \"mynull\": {\n" +
                "      \"type\": \"null\"\n" +
                "    },\n" +
                "    \"myint\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"mynumericlong\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"mystringlong\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"myfloat\": {\n" +
                "      \"type\": \"number\"\n" +
                "    },\n" +
                "    \"mydouble\": {\n" +
                "      \"type\": \"number\"\n" +
                "    },\n" +
                "    \"myboolean\": {\n" +
                "      \"type\": \"boolean\"\n" +
                "    },\n" +
                "    \"mystring\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"mybinary\": {\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    \"mysuit\": {\n" +
                "      \"$ref\": \"#/definitions/Suits\"\n" +
                "    },\n" +
                "    \"mysuit2\": {\n" +
                "      \"$ref\": \"#/definitions/Suits\"\n" +
                "    },\n" +
                "    \"myarray\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"items\": {\n" +
                "        \"type\": \"string\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"mymap\": {\n" +
                "      \"type\": \"object\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"definitions\": {\n" +
                "    \"Suits\": {\n" +
                "      \"enum\": [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]\n" +
                "    }\n" +
                "  }\n" +
                "}\n'";

        String serdes = "'t1=json:{\"type\":\"object\"," +
            "\"properties\":{\"f1\":{\"type\":\"string\"}}}'," +
            "'t2=json:{ \"type\": \"object\", \"properties\": { \"f1\": { \"type\": \"string\" }," +
            " \"nested\": { \"type\": \"object\", \"properties\": { \"f2\": { \"type\": " +
            "\"string\" } } } } }'";

        props.put(KGiraffeConfig.VALUE_SERDES_CONFIG, serdes + types + cycle);
    }
}
