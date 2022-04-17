
![logo](./resources/kgiraffe-small.png)

# kgiraffe

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/kgiraffe/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kgiraffe/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.kgraph/kgiraffe-core.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Ckgiraffe-core
[javadoc-shield]: https://javadoc.io/badge/io.kgraph/kgiraffe-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kgraph/kgiraffe-core

kgiraffe is like [kcat](https://github.com/edenhill/kcat) (formerly kafkacat), but with a GraphQL interface

## Features

kgiraffe wraps the following functionality with a GraphQL interface:

### Topic Management

- Query topics
- Publish to topics
- Subscribe to topics
- Full support for Avro, Json Schema, and Protobuf records

### Schema Management

- Validate and stage schemas
- Query schemas and subjects
- Register schemas
- Test schema compatibility

###

```

Usage: kgiraffe [-hV] [-F=<config-file>] [-m=<ms>] [-o=<offset>] [-r=<url>]
                [-b=<broker>]... [-k=<topic=serde>]... [-p=<partition>]...
                [-s=<serde>]... [-t=<topic>]... [-v=<topic=serde>]...
                [-X=<prop=val>]...
A GraphQL Interface for Apache Kafka.
  -t, --topic=<topic>               Topic(s) to consume from and produce to
  -p, --partition=<partition>       Partition(s)
  -b, --bootstrap-server=<broker>   Bootstrap broker(s) (host:[port])
  -m, --metadata-timeout=<ms>       Metadata (et.al.) request timeout
  -F, --file=<config-file>          Read configuration properties from file
  -o, --offset=<offset>             Offset to start consuming from:
                                      beginning | end |
                                      <value>  (absolute offset) |
                                      -<value> (relative offset from end)
                                      @<value> (timestamp in ms to start at)
                                      Default: beginning
  -k, --key-serde=<topic=serde>     (De)serialize keys using <serde>
  -v, --value-serde=<topic=serde>   (De)serialize values using <serde>
                                    Available serdes:
                                      short | int | long | float |
                                      double | string | binary |
                                      avro:<schema|@file> |
                                      json:<schema|@file> |
                                      proto:<schema|@file> |
                                      latest (use latest version in SR) |
                                      <id>   (use schema id from SR)
                                      Default for key:   binary
                                      Default for value: latest
                                    The avro/json/proto serde formats can
                                    also be specified with refs, e.g.
                                      avro:<schema|@file>;refs:<refs|@file>
                                    where refs are schema references
                                    of the form
                                      [{name=<name>,subject=<subject>,
                                        version=<version>},..]
  -r, --schema-registry-url=<url>   SR (Schema Registry) URL
  -s, --stage-schema=<serde>        Validate and stage the given schema(s).
                                    See avro/json/proto serde formats above.
  -X, --property=<prop=val>         Set kgiraffe configuration property.
  -h, --help                        Show this help message and exit.
  -V, --version                     Print version information and exit.

```

## Getting Started

To run kgiraffe, download a [release](https://github.com/rayokota/kgiraffe/releases), unpack it, 
and then modify `config/kgiraffe.properties` to point to an existing Kafka broker.  Then run 
the following:

```bash
$ bin/kgiraffe -F config/kgiraffe.properties
```




## Basic Configuration

kgiraffe has a number of configuration properties that can be specified.  

- `listeners` - List of listener URLs that include the scheme, host, and port.  Defaults to `http://0.0.0.0:2379`.  
- `cluster.group.id` - The group ID to be used for leader election.  Defaults to `kgiraffe`.
- `leader.eligibility` - Whether this node can participate in leader election.  Defaults to true.
- `kafkacache.backing.cache` - The backing cache for KCache, one of `memory` (default), `bdbje`, `lmdb`, `mapdb`, or `rocksdb`.
- `kafkacache.data.dir` - The root directory for backing cache storage.  Defaults to `/tmp`.
- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumers, which needs to be unique for each node.  Defaults to `kgiraffe-1`.
- `kafkacache.topic.replication.factor` - The replication factor for the internal topics created by kgiraffe.  Defaults to 3.
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

kgiraffe supports the same authentication and role-based access control (RBAC) APIs as etcd.  For more info, see the etcd documentation [here](https://etcd.io/docs/v3.4.0/op-guide/authentication/).


### Kafka Authentication

Authentication to a secure Kafka cluster is described [here](https://github.com/rayokota/kcache#security).
 
## Implementation Notes



For more info on kgiraffe, see this [blog post](...).
