package org.apache.flink.formats.registry.confluent;

import com.google.common.collect.ImmutableMap;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

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
        org.apache.kafka.connect.data.Schema connectSchema = null;

        if (schema.schemaType().equals(ProtobufSchema.TYPE)) {
            connectSchema = convertProtobufSchema((ProtobufSchema) schema);
        } else if (schema.schemaType().equals(AvroSchema.TYPE)) {
            connectSchema = convertAvroSchema((AvroSchema) schema);
        } else if (schema.schemaType().equals(JsonSchema.TYPE)) {
            connectSchema = convertJsonSchema((JsonSchema) schema);
        }

        return connectToFlink(connectSchema);
    }

    private org.apache.kafka.connect.data.Schema convertAvroSchema(AvroSchema schema) {
        return new AvroData(new AvroDataConfig(ImmutableMap.of())).toConnectSchema(schema.rawSchema());
    }

    private org.apache.kafka.connect.data.Schema convertJsonSchema(JsonSchema schema) {
        return new JsonSchemaData(new JsonSchemaDataConfig(
                ImmutableMap.of(
                        JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()
                )
        )).toConnectSchema(schema);
    }

    private org.apache.kafka.connect.data.Schema convertProtobufSchema(ProtobufSchema schema) {
        return new ProtobufData(new ProtobufDataConfig(ImmutableMap.of())).toConnectSchema(schema);
    }

    private Schema connectToFlink(org.apache.kafka.connect.data.Schema connectSchema) {
        Schema.Builder builder = Schema.newBuilder();

        connectSchema.fields().forEach(field -> {
            builder.column(field.name(), connectToFlinkType(field.schema()));
        });

        return builder.build();
    }

    private DataType connectToFlinkType(org.apache.kafka.connect.data.Schema type) {
        if (type.type().isPrimitive()) {
            switch (type.type()) {
                case INT8:
                    return DataTypes.TINYINT();
                case INT16:
                    return DataTypes.SMALLINT();
                case INT32:
                    if (type.name() != null && type.name().equals("org.apache.kafka.connect.data.Time")) {
                        return DataTypes.TIME();
                    } else if (type.name() != null && type.name().equals("org.apache.kafka.connect.data.Date")) {
                        return DataTypes.DATE();
                    }
                    else {
                        return DataTypes.INT();
                    }
                case INT64:
                    if (type.name() != null && type.name().equals("org.apache.kafka.connect.data.Timestamp")) {
                        return DataTypes.TIMESTAMP();
                    }
                    else {
                        return DataTypes.BIGINT();
                    }
                case FLOAT32:
                    return DataTypes.FLOAT();
                case FLOAT64:
                    return DataTypes.DOUBLE();
                case BOOLEAN:
                    return DataTypes.BOOLEAN();
                case STRING:
                    return DataTypes.STRING();
                case BYTES:
                    return type.name() != null && type.name().equals("org.apache.kafka.connect.data.Decimal")
                            ? DataTypes.DECIMAL(
                                    Integer.parseInt(type.parameters().get("connect.decimal.precision")),
                                    Integer.parseInt(type.parameters().get("scale")))
                            : DataTypes.BYTES();
                default:
                    return null;
            }
        } else {
            switch (type.type()) {
                case MAP:
                    return DataTypes.MAP(connectToFlinkType(type.keySchema()), connectToFlinkType(type.valueSchema()));
                case ARRAY:
                    return DataTypes.ARRAY(connectToFlinkType(type.valueSchema()));
                case STRUCT:
                    final DataTypes.Field[] fields = new DataTypes.Field[type.fields().size()];
                    for (int i = 0; i < type.fields().size(); i++) {
                        final Field field = type.fields().get(i);
                        fields[i] = DataTypes.FIELD(field.name(), connectToFlinkType(field.schema()));
                    }
                    return DataTypes.ROW(fields).notNull();
                default:
                    return null;
            }
        }
    }
}
