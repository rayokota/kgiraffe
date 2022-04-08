/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kgraph.kgiraffe.serialization;

import io.vavr.Tuple2;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;
import java.util.Optional;

public class ValueDeserializer implements Deserializer<Tuple2<Optional<Headers>, Bytes>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Tuple2<Optional<Headers>, Bytes> deserialize(String s, Headers headers, byte[] bytes) {
        return new Tuple2<>(Optional.of(headers), Bytes.wrap(bytes));
    }

    @Override
    public Tuple2<Optional<Headers>, Bytes> deserialize(String s, byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
}
