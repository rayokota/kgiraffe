# KGiraffe

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/kgiraffe/workflows/build/badge.svg?branch=master
[github-actions-link]: https://github.com/rayokota/kgiraffe/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.kgraph/kgiraffe-core.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Ckgiraffe-core
[javadoc-shield]: https://javadoc.io/badge/io.kgraph/kgiraffe-core.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.kgraph/kgiraffe-core

KGiraffe is a ...

## Getting Started

To run KGiraffe, download a [release](https://github.com/rayokota/kgiraffe/releases), unpack it, 
and then modify `config/kgiraffe.properties` to point to an existing Kafka broker.  Then run 
the following:

```bash
$ bin/kgiraffe -F config/kgiraffe.properties
```




## Basic Configuration

KGiraffe has a number of configuration properties that can be specified.  

- `listeners` - List of listener URLs that include the scheme, host, and port.  Defaults to `http://0.0.0.0:2379`.  
- `cluster.group.id` - The group ID to be used for leader election.  Defaults to `kgiraffe`.
- `leader.eligibility` - Whether this node can participate in leader election.  Defaults to true.
- `kafkacache.backing.cache` - The backing cache for KCache, one of `memory` (default), `bdbje`, `lmdb`, `mapdb`, or `rocksdb`.
- `kafkacache.data.dir` - The root directory for backing cache storage.  Defaults to `/tmp`.
- `kafkacache.bootstrap.servers` - A list of host and port pairs to use for establishing the initial connection to Kafka.
- `kafkacache.group.id` - The group ID to use for the internal consumers, which needs to be unique for each node.  Defaults to `kgiraffe-1`.
- `kafkacache.topic.replication.factor` - The replication factor for the internal topics created by KGiraffe.  Defaults to 3.
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

KGiraffe supports the same authentication and role-based access control (RBAC) APIs as etcd.  For more info, see the etcd documentation [here](https://etcd.io/docs/v3.4.0/op-guide/authentication/).


### Kafka Authentication

Authentication to a secure Kafka cluster is described [here](https://github.com/rayokota/kcache#security).
 
## Implementation Notes



For more info on KGiraffe, see this [blog post](...).
