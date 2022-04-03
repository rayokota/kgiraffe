/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.kgraph.kgiraffe.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.TreeMap;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * A utility class for Jackson.
 */
public class Jackson {
  private Jackson() {
    /* singleton */
  }

  /**
   * Creates a new {@link com.fasterxml.jackson.databind.ObjectMapper}.
   */
  public static ObjectMapper newObjectMapper() {
    return newObjectMapper(false);
  }

  /**
   * Creates a new {@link com.fasterxml.jackson.databind.ObjectMapper}.
   *
   * @param sorted whether to sort object properties
   */
  public static ObjectMapper newObjectMapper(boolean sorted) {
    final ObjectMapper mapper = JsonMapper.builder()
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES) // for simpler ref specifications
        .build();

    return configure(mapper, sorted);
  }

  /**
   * Creates a new {@link com.fasterxml.jackson.databind.ObjectMapper} with a custom
   * {@link com.fasterxml.jackson.core.JsonFactory}.
   *
   * @param jsonFactory instance of {@link com.fasterxml.jackson.core.JsonFactory} to use
   *     for the created {@link com.fasterxml.jackson.databind.ObjectMapper} instance.
   */
  public static ObjectMapper newObjectMapper(JsonFactory jsonFactory) {
    final ObjectMapper mapper = JsonMapper.builder(jsonFactory)
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
        .build();

    return configure(mapper, false);
  }

  private static ObjectMapper configure(ObjectMapper mapper, boolean sorted) {
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setNodeFactory(sorted
        ? new SortingNodeFactory(true)
        : JsonNodeFactory.withExactBigDecimals(true));

    return mapper;
  }

  static class SortingNodeFactory extends JsonNodeFactory {
    public SortingNodeFactory(boolean bigDecimalExact) {
      super(bigDecimalExact);
    }

    @Override
    public ObjectNode objectNode() {
      return new ObjectNode(this, new TreeMap<>());
    }
  }
}
