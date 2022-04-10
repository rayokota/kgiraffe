/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kgraph.kgiraffe;

import io.kcache.KafkaCacheConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public class KGiraffeConfig extends KafkaCacheConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KGiraffeConfig.class);

    // TODO
    public static final String LISTENER_CONFIG = "listener";
    public static final String LISTENER_DEFAULT = "http://0.0.0.0:8765";
    public static final String LISTENER_DOC =
        "The URL for kgiraffe to listen on. The listener must include the protocol, "
            + "hostname, and port. For example: http://myhost:8765, https://0.0.0.0:8765";

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_URL_DOC =
        "Comma-separated list of URLs for schema registry instances that can be used to register "
            + "or look up schemas.";

    public static final String TOPICS_CONFIG = "topics";
    public static final String TOPICS_DOC = "Comma-separated list of topics.";

    public static final String GRAPHQL_MAX_COMPLEXITY_CONFIG = "graphql.max.complexity";
    public static final int GRAPHQL_MAX_COMPLEXITY_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_MAX_COMPLEXITY_DOC =
        "The maximum complexity of the fields for a GraphQL query.";

    public static final String GRAPHQL_MAX_DEPTH_CONFIG = "graphql.max.depth";
    public static final int GRAPHQL_MAX_DEPTH_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_MAX_DEPTH_DOC =
        "The maximum depth for a GraphQL query.";

    public static final String GRAPHQL_TIMEOUT_MS_CONFIG = "graphql.timeout.ms";
    public static final int GRAPHQL_TIMEOUT_MS_DEFAULT = Integer.MAX_VALUE;
    public static final String GRAPHQL_TIMEOUT_MS_DOC =
        "The timeout in ms for a GraphQL query.";

    public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    public static final String SSL_KEYSTORE_LOCATION_DOC =
        "Location of the keystore file to use for SSL. This is required for HTTPS.";
    public static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    public static final String SSL_KEYSTORE_PASSWORD_DOC =
        "The store password for the keystore file.";
    public static final String SSL_KEYSTORE_PASSWORD_DEFAULT = "";

    public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    public static final String SSL_KEY_PASSWORD_DOC =
        "The password of the private key in the keystore file.";
    public static final String SSL_KEY_PASSWORD_DEFAULT = "";

    public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    public static final String SSL_KEYSTORE_TYPE_DOC =
        "The type of keystore file.";

    public static final String SSL_STORE_TYPE_JKS = "JKS";
    public static final String SSL_STORE_TYPE_PKCS12 = "PKCS12";
    public static final ConfigDef.ValidString SSL_STORE_TYPE_VALIDATOR =
        ConfigDef.ValidString.in(
            SSL_STORE_TYPE_JKS,
            SSL_STORE_TYPE_PKCS12
        );

    public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
    public static final String SSL_KEYMANAGER_ALGORITHM_DOC =
        "The algorithm used by the key manager factory for SSL connections. "
            + "Leave blank to use Jetty's default.";
    public static final String SSL_KEYMANAGER_ALGORITHM_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    public static final String SSL_TRUSTSTORE_LOCATION_DOC =
        "Location of the trust store. Required only to authenticate HTTPS clients.";
    public static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    public static final String SSL_TRUSTSTORE_PASSWORD_DOC =
        "The store password for the trust store file.";
    public static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "";

    public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    public static final String SSL_TRUSTSTORE_TYPE_DOC =
        "The type of trust store file.";
    public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";

    public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC =
        "The algorithm used by the trust manager factory for SSL connections. "
            + "Leave blank to use Jetty's default.";
    public static final String SSL_TRUSTMANAGER_ALGORITHM_DEFAULT = "";

    public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
    public static final String SSL_PROTOCOL_DOC =
        "The SSL protocol used to generate the SslContextFactory.";
    public static final String SSL_PROTOCOL_DEFAULT = "TLS";

    public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
    public static final String SSL_PROVIDER_DOC =
        "The SSL security provider name. Leave blank to use Jetty's default.";
    public static final String SSL_PROVIDER_DEFAULT = "";

    public static final String SSL_CLIENT_AUTHENTICATION_CONFIG = "ssl.client.authentication";
    public static final String SSL_CLIENT_AUTHENTICATION_NONE = "NONE";
    public static final String SSL_CLIENT_AUTHENTICATION_REQUESTED = "REQUESTED";
    public static final String SSL_CLIENT_AUTHENTICATION_REQUIRED = "REQUIRED";
    public static final String SSL_CLIENT_AUTHENTICATION_DOC =
        "SSL mutual auth. Set to NONE to disable SSL client authentication, set to REQUESTED to "
            + "request but not require SSL client authentication, and set to REQUIRED to require SSL "
            + "client authentication.";
    public static final ConfigDef.ValidString SSL_CLIENT_AUTHENTICATION_VALIDATOR =
        ConfigDef.ValidString.in(
            SSL_CLIENT_AUTHENTICATION_NONE,
            SSL_CLIENT_AUTHENTICATION_REQUESTED,
            SSL_CLIENT_AUTHENTICATION_REQUIRED
        );

    public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
    public static final String SSL_ENABLED_PROTOCOLS_DOC =
        "The list of protocols enabled for SSL connections. Comma-separated list. "
            + "Leave blank to use Jetty's defaults.";
    public static final String SSL_ENABLED_PROTOCOLS_DEFAULT = "";

    public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
    public static final String SSL_CIPHER_SUITES_DOC =
        "A list of SSL cipher suites. Leave blank to use Jetty's defaults.";
    public static final String SSL_CIPHER_SUITES_DEFAULT = "";

    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
        "ssl.endpoint.identification.algorithm";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC =
        "The endpoint identification algorithm to validate the server hostname using the "
            + "server certificate.";
    public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT = null;

    public static final String TOKEN_TYPE_CONFIG = "token.type";
    public static final String TOKEN_TYPE_DOC =
        "The token type, either simple (for single-node cluster testing) or jwt.";
    public static final String TOKEN_TYPE_SIMPLE = "simple";
    public static final String TOKEN_TYPE_JWT = "jwt";
    public static final ConfigDef.ValidString TOKEN_TYPE_VALIDATOR =
        ConfigDef.ValidString.in(
            TOKEN_TYPE_SIMPLE,
            TOKEN_TYPE_JWT
        );

    public static final String TOKEN_PUBLIC_KEY_PATH_CONFIG = "token.public.key.path";
    public static final String TOKEN_PUBLIC_KEY_PATH_DOC =
        "Location of a PEM encoded public key for verifying tokens.";

    public static final String TOKEN_PRIVATE_KEY_PATH_CONFIG = "token.private.key.path";
    public static final String TOKEN_PRIVATE_KEY_PATH_DOC =
        "Location of a PEM encoded private key for signing tokens.";

    public static final String TOKEN_SIGNATURE_ALGORITHM_CONFIG = "token.signature.algorithm";
    public static final String TOKEN_SIGNATURE_ALGORITHM_DEFAULT = "RS256";
    public static final ConfigDef.ValidString TOKEN_SIGNATURE_ALGORITHM_VALIDATOR =
        ConfigDef.ValidString.in("RS256");
    public static final String TOKEN_SIGNATURE_ALGORITHM_DOC =
        "Signature scheme to be used when signing/verifying tokens"
            + " as defined in https://tools.ietf.org/html/rfc7518#section-3.1."
            + " Currently only RS256 is supported.";

    public static final String TOKEN_TTL_SECS_CONFIG = "token.ttl.secs";
    public static final int TOKEN_TTL_SECS_DEFAULT = 300;
    public static final String TOKEN_TTL_SECS_DOC = "Time-to-live for tokens.";

    private static final ConfigDef config;

    static {
        config = baseConfigDef()
            .define(
                LISTENER_CONFIG,
                Type.STRING,
                LISTENER_DEFAULT,
                Importance.HIGH,
                LISTENER_DOC
            ).define(SCHEMA_REGISTRY_URL_CONFIG,
                Type.LIST,
                null,
                Importance.HIGH,
                SCHEMA_REGISTRY_URL_DOC
            ).define(TOPICS_CONFIG,
                Type.LIST,
                null,
                Importance.HIGH,
                TOPICS_DOC
            ).define(
                GRAPHQL_MAX_COMPLEXITY_CONFIG,
                Type.INT,
                GRAPHQL_MAX_COMPLEXITY_DEFAULT,
                Importance.LOW,
                GRAPHQL_MAX_COMPLEXITY_DOC
            ).define(
                GRAPHQL_MAX_DEPTH_CONFIG,
                Type.INT,
                GRAPHQL_MAX_DEPTH_DEFAULT,
                Importance.LOW,
                GRAPHQL_MAX_DEPTH_DOC
            ).define(
                GRAPHQL_TIMEOUT_MS_CONFIG,
                Type.INT,
                GRAPHQL_TIMEOUT_MS_DEFAULT,
                Importance.LOW,
                GRAPHQL_TIMEOUT_MS_DOC
            ).define(
                SSL_KEYSTORE_LOCATION_CONFIG,
                Type.STRING,
                SSL_KEYSTORE_LOCATION_DEFAULT,
                Importance.HIGH,
                SSL_KEYSTORE_LOCATION_DOC
            ).define(
                SSL_KEYSTORE_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_KEYSTORE_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_KEYSTORE_PASSWORD_DOC
            ).define(
                SSL_KEY_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_KEY_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_KEY_PASSWORD_DOC
            ).define(
                SSL_KEYSTORE_TYPE_CONFIG,
                Type.STRING,
                SSL_STORE_TYPE_JKS,
                SSL_STORE_TYPE_VALIDATOR,
                Importance.MEDIUM,
                SSL_KEYSTORE_TYPE_DOC
            ).define(
                SSL_KEYMANAGER_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_KEYMANAGER_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_KEYMANAGER_ALGORITHM_DOC
            ).define(
                SSL_TRUSTSTORE_LOCATION_CONFIG,
                Type.STRING,
                SSL_TRUSTSTORE_LOCATION_DEFAULT,
                Importance.HIGH,
                SSL_TRUSTSTORE_LOCATION_DOC
            ).define(
                SSL_TRUSTSTORE_PASSWORD_CONFIG,
                Type.PASSWORD,
                SSL_TRUSTSTORE_PASSWORD_DEFAULT,
                Importance.HIGH,
                SSL_TRUSTSTORE_PASSWORD_DOC)
            .define(
                SSL_TRUSTSTORE_TYPE_CONFIG,
                Type.STRING,
                SSL_TRUSTSTORE_TYPE_DEFAULT,
                Importance.MEDIUM,
                SSL_TRUSTSTORE_TYPE_DOC)
            .define(
                SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_TRUSTMANAGER_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_TRUSTMANAGER_ALGORITHM_DOC
            ).define(
                SSL_PROTOCOL_CONFIG,
                Type.STRING,
                SSL_PROTOCOL_DEFAULT,
                Importance.MEDIUM,
                SSL_PROTOCOL_DOC)
            .define(
                SSL_PROVIDER_CONFIG,
                Type.STRING,
                SSL_PROVIDER_DEFAULT,
                Importance.MEDIUM,
                SSL_PROVIDER_DOC
            ).define(
                SSL_CLIENT_AUTHENTICATION_CONFIG,
                Type.STRING,
                SSL_CLIENT_AUTHENTICATION_NONE,
                SSL_CLIENT_AUTHENTICATION_VALIDATOR,
                Importance.MEDIUM,
                SSL_CLIENT_AUTHENTICATION_DOC
            ).define(
                SSL_ENABLED_PROTOCOLS_CONFIG,
                Type.LIST,
                SSL_ENABLED_PROTOCOLS_DEFAULT,
                Importance.MEDIUM,
                SSL_ENABLED_PROTOCOLS_DOC
            ).define(
                SSL_CIPHER_SUITES_CONFIG,
                Type.LIST,
                SSL_CIPHER_SUITES_DEFAULT,
                Importance.LOW,
                SSL_CIPHER_SUITES_DOC
            ).define(
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
                Type.STRING,
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DEFAULT,
                Importance.LOW,
                SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_DOC
            ).define(
                TOKEN_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                TOKEN_TYPE_SIMPLE,
                TOKEN_TYPE_VALIDATOR,
                ConfigDef.Importance.HIGH,
                TOKEN_TYPE_DOC
            ).define(
                TOKEN_PUBLIC_KEY_PATH_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                TOKEN_PUBLIC_KEY_PATH_DOC
            ).define(
                TOKEN_PRIVATE_KEY_PATH_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                TOKEN_PRIVATE_KEY_PATH_DOC
            ).define(
                TOKEN_SIGNATURE_ALGORITHM_CONFIG,
                ConfigDef.Type.STRING,
                TOKEN_SIGNATURE_ALGORITHM_DEFAULT,
                TOKEN_SIGNATURE_ALGORITHM_VALIDATOR,
                ConfigDef.Importance.LOW,
                TOKEN_SIGNATURE_ALGORITHM_DOC
            ).define(
                TOKEN_TTL_SECS_CONFIG,
                ConfigDef.Type.INT,
                TOKEN_TTL_SECS_DEFAULT,
                ConfigDef.Importance.LOW,
                TOKEN_TTL_SECS_DOC
            );
    }

    public KGiraffeConfig(File propsFile) {
        super(config, getPropsFromFile(propsFile));
    }

    public KGiraffeConfig(Map<?, ?> props) {
        super(config, props);
    }

    public List<String> getSchemaRegistryUrls() {
        return getList(SCHEMA_REGISTRY_URL_CONFIG);
    }

    public List<String> getTopics() {
        return getList(TOPICS_CONFIG);
    }

    public int getGraphQLMaxComplexity() {
        return getInt(GRAPHQL_MAX_COMPLEXITY_CONFIG);
    }

    public int getGraphQLMaxDepth() {
        return getInt(GRAPHQL_MAX_DEPTH_CONFIG);
    }

    public int getGraphQLTimeoutMs() {
        return getInt(GRAPHQL_TIMEOUT_MS_CONFIG);
    }

    private static String getDefaultHost() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            throw new ConfigException("Unknown local hostname", e);
        }
    }

    public static Properties getPropsFromFile(File propsFile) throws ConfigException {
        Properties props = new Properties();
        if (propsFile == null) {
            return props;
        }
        try (FileInputStream propStream = new FileInputStream(propsFile)) {
            props.load(propStream);
        } catch (IOException e) {
            throw new ConfigException("Couldn't load properties from " + propsFile, e);
        }
        return props;
    }

    public enum SerdeType {
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        STRING,
        BINARY,
        LATEST,
        ID
    }

    public static class Serde {
        private final SerdeType serdeType;
        private final int id;

        public Serde(String value) {
            SerdeType serdeType;
            int id;
            try {
                serdeType = SerdeType.valueOf(value.toUpperCase(Locale.ROOT));
                id = 0;
            } catch (IllegalArgumentException e) {
                try {
                    id = Integer.parseInt(value);
                    serdeType = SerdeType.ID;
                } catch (NumberFormatException nfe) {
                    throw new ConfigException("Couldn't parse serde: " + value, nfe);
                }
            }
            this.serdeType = serdeType;
            this.id = id;
        }

        public Serde(SerdeType serdeType, int id) {
            this.serdeType = serdeType;
            this.id = id;
        }

        public SerdeType getSerdeType() {
            return serdeType;
        }

        public int getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Serde serde = (Serde) o;
            return id == serde.id && serdeType == serde.serdeType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(serdeType, id);
        }

        @Override
        public String toString() {
            if (serdeType == SerdeType.ID) {
                return String.valueOf(id);
            } else {
                return serdeType.name().toLowerCase(Locale.ROOT);
            }
        }
    }
}