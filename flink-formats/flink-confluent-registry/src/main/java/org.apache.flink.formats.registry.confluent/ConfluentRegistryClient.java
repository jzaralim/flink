package org.apache.flink.formats.registry.confluent;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema.Type;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

public class ConfluentRegistryClient {

    private static final int CACHE_SIZE = 1000;

    SchemaRegistryClient srClient;

    public ConfluentRegistryClient(SchemaRegistryClient srClient) {
        this.srClient = srClient;
    }

    public static ConfluentRegistryClient createClient(String url, Map<String, ?> properties) {
        return new ConfluentRegistryClient(new CachedSchemaRegistryClient(url, CACHE_SIZE, properties));
    }

    public Schema getSchema(int id) throws IOException, RestClientException {
        ParsedSchema schema = srClient.getSchemaById(id);

        if (schema.schemaType().equals(ProtobufSchema.TYPE)) {
            return convertProtobufSchema((ProtobufSchema) schema);
        } else if (schema.schemaType().equals(AvroSchema.TYPE)) {
            return convertAvroSchema((AvroSchema) schema);
        } else if (schema.schemaType().equals(JsonSchema.TYPE)) {
            return convertJsonSchema((JsonSchema) schema);
        }
        return null;
    }

    private Schema convertAvroSchema(AvroSchema avroSchema) {
        Schema.Builder builder = Schema.newBuilder();
        avroSchema.rawSchema().getFields().forEach(field -> {
            String avroType;
            if (field.schema().isUnion()) {
                avroType = field.schema().getTypes().stream()
                        .filter(type -> !type.getType().equals(Type.NULL))
                        .findFirst().get().getName();
            } else {
                avroType = field.schema().getType().getName();
            }
            builder.column(field.name(), DataTypes.of(avroType));
        });
        return builder.build();
    }

    private Schema convertJsonSchema(JsonSchema jsonSchema) {
        Schema.Builder builder = Schema.newBuilder();
        jsonSchema.toJsonNode().get("properties").fields().forEachRemaining(field -> {
            builder.column(field.getKey(), field.getValue().toString());
        });
        return builder.build();
    }

    private Schema convertProtobufSchema(ProtobufSchema protobufSchema) {
        Schema.Builder builder = Schema.newBuilder();
        protobufSchema.toDescriptor().getFields().forEach(fieldDescriptor -> {
            builder.column(fieldDescriptor.getName(), fieldDescriptor.getJavaType().toString());
        });
        return builder.build();
    }
}
