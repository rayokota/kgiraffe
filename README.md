# Kafka GraphQL

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/kafka-graphql/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kafka-graphql/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.kgraph/kafka-graphql-core.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Ckafka-graphql-core
[javadoc-shield]: https://javadoc.io/badge/io.kgraph/kafka-graphql-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kgraph/kafka-graphql-core

Kafka GraphQL is a ...

## Maven

Releases of Kafka GraphQL are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.kgraph</groupId>
    <artifactId>kafka-graphql-core</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Getting Started

To run Kafka GraphQL, download a [release](https://github.com/rayokota/kafka-graphql/releases), unpack it, 
and then modify `config/kafka-graphql.properties` to point to an existing Kafka broker.  Then run 
the following:

```bash
$ bin/kafka-graphql-start config/kafka-graphql.properties
```




## Basic Configuration

Kafka GraphQL has a number of configuration properties that can be specified.  

- `listeners` - List of listener URLs that include the scheme, host, and port.  Defaults to `http://0.0.0.0:2379`.  
- `cluster.group.id` - The group ID to be used for leader election.  Defaults to `kafka-graphql`.
- `leader.eligibility` - Whether this node can participate in leader election.  Defaults to true.
- `kafkacache.backing.cache` - The backing cache for KCache, one of `memory` (default), `bdbje`, `lmdb`, `mapdb`, or `rocksdb`.
- `kafkacache.data.dir` - The root directory for backing cache storage.  Defaults to `/tmp`.
- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumers, which needs to be unique for each node.  Defaults to `kafka-graphql-1`.
- `kafkacache.topic.replication.factor` - The replication factor for the internal topics created by Kafka GraphQL.  Defaults to 3.
- `kafkacache.init.timeout.ms` - The timeout for initialization of the Kafka cache, including creation of internal topics.  Defaults to 300 seconds.
- `kafkacache.timeout.ms` - The timeout for an operation on the Kafka cache.  Defaults to 60 seconds.

## Security

### HTTPS

To use HTTPS, first configure the `listeners` with an `https` prefix, then specify the following properties with the appropriate values.

```
ssl.keystore.location=/var/private/ssl/custom.keystore
ssl.keystore.password=changeme
ssl.key.password=changeme
ssl.truststore.location=/var/private/ssl/custom.truststore
ssl.truststore.password=changeme
```


### Authentication and Role-Based Access Control

Kafka GraphQL supports the same authentication and role-based access control (RBAC) APIs as etcd.  For more info, see the etcd documentation [here](https://etcd.io/docs/v3.4.0/op-guide/authentication/).


### Kafka Authentication

Authentication to a secure Kafka cluster is described [here](https://github.com/rayokota/kcache#security).
 
## Implementation Notes



For more info on Kafka GraphQL, see this [blog post](...).
