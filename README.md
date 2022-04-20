
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
- Test schema compatibility
- Query schemas and subjects
- Register schemas


###


## Getting Started

To run kgiraffe, download a [release](https://github.com/rayokota/kgiraffe/releases), unpack it.
Then change to the `kgiraffe-${version}` directory and run the following to see the command-line options:

```bash
$ bin/kgiraffe -h
Usage: kgiraffe [-hV] [-F=<config-file>] [-m=<ms>] [-o=<offset>] [-r=<url>]
                [-b=<broker>]... [-k=<topic=serde>]... [-p=<partition>]...
                [-s=<serde>]... [-t=<topic>]... [-v=<topic=serde>]...
                [-X=<prop=val>]...
A GraphQL Interface for Apache Kafka and Schema Registry.
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

kgiraffe shares many command-line options with kcat.  In addition, a file
containing configuration properties can be used.  Simply modify 
`config/kgiraffe.properties` to point to an existing Kafka broker and Schema
Registry. Then run the following:

```bash
# Run with properties file
$ bin/kgiraffe -F config/kgiraffe.properties
```

When kgiraffe starts, it will generate a GraphQL schema that can be used to read,
write, and subscribe to your topics.  In addition, kgiraffe will allow you to
validate and stage schemas, as well as test them for compatibility, before you
register them to Schema Registry.

Once kgiraffe is running, browse to http://localhost:8765/kgiraffe to launch
the GraphQL Playground.

## Command Line Examples

### Topic Management

Generate a GraphQL schema for Kafka `mytopic` topic using Schema Registry.

```bash
$ kgiraffe -b mybroker -t mytopic -r http://schema-registry-url:8081
```

Generate a GraphQL schema for Kafka `mytopic` topic, where the schema for the
value is constructed from schema 123 in Schema Registry.

```bash
$ kgiraffe -b mybroker -t mytopic -r http://schema-registry-url:8081 -v mytopic=123
```

Generate a GraphQL schema for Kafka `mytopic` topic, where the schema for the
value is constructed from the given Avro schema.

```bash
$ kgiraffe -b mybroker -t mytopic -r http://schema-registry-url:8081 -X auto.register.schemas=true \
    -v mytopic='avro:{"type":"record","name":"myrecord","fields":[{"name":"field1","type":"string"}]}'
```

Generate a GraphQL schema for Kafka `mytopic` topic, where the schema for the
value is constructed from the given Avro schema file.

```bash
$ kgiraffe -b mybroker -t mytopic -r http://schema-registry-url:8081 -X auto.register.schemas=true \
    -v mytopic=avro:@schema.avro
```

### Schema Management

Validate and stage the given Avro schema. The validation result will be in the 
`validation_error` GraphQL field.

```bash
$ kgiraffe -r http://schema-registry-url:8081 \
    -s 'avro: {"type":"record","name":"myrecord","fields":[{"name":"field1","type":"string"}]}'
```

Validate and stage the given Avro schema file.

```bash
$ kgiraffe -r http://schema-registry-url:8081 -s avro:@schema.avro
````

## GraphQL Examples


### Topic Management

Query records for Kafka `mytopic` topic, using the generated schema.

```graphql
query {
  mytopic {
    value {
      field1 
    }
    topic
    offset
    partition
    ts
  }
}
```
Query records for Kafka `mytopic` topic with the given field value.

```graphql
query {
  mytopic (where: {value: {field1: {_eq: "hello"}}}) {
    value {
      field1 
    }
    topic
    offset
    partition
    ts
  }
}
```

Publish records to Kafka `mytopic` topic with the given value.

```graphql
mutation {
  mytopic (value: {field1: "world"}) {
    value {
      field1 
    }
    topic
    offset
    partition
    ts
  }
}
```

Publish records to Kafka `mytopic` topic, with the given headers, key, and value.


```graphql
mutation {
  mytopic ( 
    headers: { header1: "myheader" }, 
    key: "mykey", 
    value: { field1: "goodbye"}
  ) {
    headers
    value {
      field1
    }
  }
}
```

Subscribe to Kafka `mytopic` topic.

```graphql
subscription {
  mytopic {
    value {
    	field1
    }
    topic
    offset
    partition
    ts
  }
}

```


### Schema Management

Validate and stage a schema.  Staged schemas will be assigned negative ids.

```graphql
mutation {
  _stage_schema (
    schema_type: "AVRO", 
    schema: "{\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}"
  ) {
    id
    schema
    status
    validation_error
  }
}
```

Test schema compatibility against a given schema.  Both staged schemas, with negative ids,
and registered schemas can be compared.

```graphql

query {
  _test_schema_compatibility (next_id: -1, prev_id: 123) {
    is_backward_compatible
    compatibility_errors
  }
}
```

Test schema compatibility against the latest version in a given subject.

```graphql

query {
  _test_schema_compatibility (next_id: -1, prev_subject: "mysubject") {
    is_backward_compatible
    compatibility_errors
  }
}
```

Query staged schemas.

```graphql
query {
  _query_staged_schemas {
    id
    schema
    status
    validation_error
  }
}
```

Query a registered schema with the given id.

```graphql
query {
  _query_registered_schemas (id: 123) {
    id
    schema
  }
}
```

Query a registered schema with the given subject and version.

```graphql
query {
  _query_registered_schemas (subject: "mysubject", version: 1) {
    id
    schema
  }
}
```

Query all registered schemas with the given subject.

```graphql
query {
  _query_registered_schemas (subject: "mysubject") {
    id
    schema
    subject
    version
  }
}
```

Query subjects.

```graphql
query {
  _query_subjects
}
```

Register a staged schema.  The staged schema will be dropped.

```graphql
mutation {
  _register_schema (id: -1, subject: "mysubject") {
    id
    schema
    subject
    version
  }
}
```

Unstage a staged schema.

```graphql
mutation {
  _unstage_schema (id: -1) {
    id
    schema
  }
}
```

### GraphQL Schema Notes

The generated GraphQL schemas follow the JSON mappings specified for 
[Avro](https://avro.apache.org/docs/current/spec.html#json_encoding) and 
[Protobuf](https://developers.google.com/protocol-buffers/docs/proto3#json). 
Note the following (the last point is not part of the specification):

- For Avro, unions are specified by a JSON object with a single property, 
where the property name is the name of the type being used.
- For Protobuf, the types `int64`, `fixed64`, and `uint64` correspond to a JSON string.
- For Protobuf, the wrapper types are represented as the JSON for the wrapped primitive type.
- For Protobuf, if a schema has multiple messages, then the JSON representation is an
object with a single property where the property name is the name of the message type 
being used, and the property value is the JSON representation of the message.  
This is similar to how an Avro union is represented.