# Kafka Connect SMTs

Contains Single Message Transformations (SMTs) for Kafka Connect

## Usage

Head to [packages](https://github.com/nejckorasa/kafka-connect-smt/packages) to view released packages.

### [MapHeader SMT](src/main/java/io/github/nejckorasa/kafka/connect/smt/transforms/MapHeader.java) 

Splits a composite header in kafka record into separate headers. 

Composite Header must be a map and stored in json format, every map entry will add a new header with key as header name and value as header value.

#### Composite header example
```json
{
  "header1": "value1",
  "header2": "value2"
}
```

#### Configuration example

```properties
# transforms pipeline 
transforms=outbox,mapHeaders

# configure MapHeader for `headers_json_map` field name
transforms.mapHeaders.type=io.github.nejckorasa.kafka.connect.smt.transforms.MapHeader
transforms.mapHeaders.header.name=headers_json_map

# make sure `headers_json_map` is added to the header by the outbox SMT
transforms.outbox.table.fields.additional.placement=headers_json_map:header
```

### [FilterOnMatchingHeader SMT](src/main/java/io/github/nejckorasa/kafka/connect/smt/transforms/FilterOnMatchingHeader.java) 

Includes or drops records with at least one header with the configured name that matches the configured value. 

#### Configuration example

```properties
# transforms pipeline 
transforms=outbox,filterOnHeader,mapHeaders

# configure filter
transforms.filterOnHeader.type=io.github.nejckorasa.kafka.connect.smt.transforms.FilterOnMatchingHeader
predicates.filterOnHeader.filter.type=exclude
predicates.filterOnHeader.name=someHeaderName
predicates.filterOnHeader.value=someHeaderValue
```

The above configuration will **exclude** records that have a header with name `someHeaderName` and value `someHeaderValue`.
