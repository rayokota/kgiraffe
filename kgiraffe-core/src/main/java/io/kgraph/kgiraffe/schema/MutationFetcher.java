package io.kgraph.kgiraffe.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.hdocdb.store.HDocumentCollection;
import io.hdocdb.store.HDocumentDB;
import io.kcache.KafkaCache;
import io.kgraph.kgiraffe.KGiraffeEngine;
import io.kgraph.kgiraffe.util.RecordHeader;
import io.kgraph.kgiraffe.util.RecordHeaders;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Bytes;
import org.ojai.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.HEADERS_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.KEY_ATTR_NAME;
import static io.kgraph.kgiraffe.schema.GraphQLSchemaBuilder.VALUE_ATTR_NAME;

public class MutationFetcher implements DataFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(MutationFetcher.class);

    private final KGiraffeEngine engine;
    private final String topic;

    public MutationFetcher(KGiraffeEngine engine, String topic) {
        this.engine = engine;
        this.topic = topic;
    }

    @Override
    public Object get(DataFetchingEnvironment env) {
        try {
            Map<String, String> headers = env.getArgument(HEADERS_ATTR_NAME);
            Object key = env.getArgument(KEY_ATTR_NAME);
            Object value = env.getArgument(VALUE_ATTR_NAME);

            RecordHeaders recordHeaders = null;
            if (headers != null) {
                List<Header> list = headers.entrySet().stream()
                    .map(e -> new RecordHeader(
                        e.getKey(), e.getValue().getBytes(StandardCharsets.UTF_8)))
                    .collect(Collectors.toList());
                recordHeaders = new RecordHeaders(list);
            }
            Bytes keyBytes = key != null ? Bytes.wrap(engine.serializeKey(topic, key)) : null;
            Bytes valueBytes = Bytes.wrap(engine.serializeValue(topic, value));

            KafkaCache<Bytes, Bytes> cache = engine.getCache(topic);
            RecordMetadata metadata = cache.put(recordHeaders, keyBytes, valueBytes).getRecordMetadata();
            String id = metadata.topic() + "-" + metadata.partition() + "-" + metadata.offset();

            HDocumentDB docdb = engine.getDocDB();
            HDocumentCollection coll = docdb.get(topic);
            Document doc = coll.findById(id);
            return doc;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
