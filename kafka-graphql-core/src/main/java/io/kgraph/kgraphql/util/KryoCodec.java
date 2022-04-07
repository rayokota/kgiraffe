package io.kgraph.kgraphql.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class KryoCodec<T> implements MessageCodec<T, T> {

    public KryoCodec() {
    }

    @Override
    public void encodeToWire(Buffer buffer, T obj) {
        buffer.appendBytes(KryoUtils.serialize(obj));
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        return KryoUtils.deserialize(buffer.getBuffer(pos, buffer.length()).getBytes());
    }

    @Override
    public T transform(T obj) {
        return obj;
    }

    @Override
    public String name() {
        return "kryo";
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
