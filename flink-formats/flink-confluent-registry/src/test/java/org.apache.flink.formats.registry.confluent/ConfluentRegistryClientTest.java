package org.apache.flink.formats.registry.confluent;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

import java.io.IOException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.CatalogManager;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfluentRegistryClientTest {
    SchemaRegistryClient srClient;
    CatalogManager manager;
    ConfluentRegistryClient client;

    @Before
    public void setUp() {
        srClient = MockSchemaRegistry.getClientForScope("");
        manager = CatalogManagerMocks.createEmptyCatalogManager();
        client = new ConfluentRegistryClient(srClient);
    }

    @Test
    public void testAvroSchemaTranslation() throws IOException, RestClientException {
        AvroSchema schema = new AvroSchema("{\n"
                + "  \"fields\": [\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"NUM\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"int\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"STREET\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"string\"\n"
                + "      ]\n"
                + "    }\n"
                + "  ],\n"
                + "  \"name\": \"KsqlDataSourceSchema\",\n"
                + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
                + "  \"type\": \"record\"\n"
                + "}");
        srClient.register("test", schema);
        ResolvedSchema flinkSchema = client.getSchema(1).resolve(manager.getSchemaResolver());
        assertThat(flinkSchema.getColumns().size()).isEqualTo(2);
        assertThat(flinkSchema.getColumns().get(0).getName()).isEqualTo("NUM");
        assertThat(flinkSchema.getColumns().get(0).getDataType()).isEqualTo(DataTypes.INT());
        assertThat(flinkSchema.getColumns().get(1).getName()).isEqualTo("STREET");
        assertThat(flinkSchema.getColumns().get(1).getDataType()).isEqualTo(DataTypes.STRING());
    }

    @Test
    public void testJsonSchemaTranslation() throws IOException, RestClientException {
        JsonSchema schema = new JsonSchema("{\n"
                + "  \"properties\": {\n"
                + "    \"NUM\": {\n"
                + "      \"connect.index\": 0,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"int32\",\n"
                + "          \"type\": \"integer\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"STREET\": {\n"
                + "      \"connect.index\": 1,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"type\": \"string\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"type\": \"object\"\n"
                + "}");
        srClient.register("test", schema);
        ResolvedSchema flinkSchema = client.getSchema(1).resolve(manager.getSchemaResolver());
        assertThat(flinkSchema.getColumns().size()).isEqualTo(2);
        assertThat(flinkSchema.getColumns().get(0).getName()).isEqualTo("NUM");
        assertThat(flinkSchema.getColumns().get(0).getDataType()).isEqualTo(DataTypes.INT());
        assertThat(flinkSchema.getColumns().get(1).getName()).isEqualTo("STREET");
        assertThat(flinkSchema.getColumns().get(1).getDataType()).isEqualTo(DataTypes.STRING());
    }

    @Test
    public void testProtobufSchemaTranslation() throws IOException, RestClientException {
        ProtobufSchema schema = new ProtobufSchema("syntax = \"proto3\";\n"
                + "\n"
                + "message ConnectDefault1 {\n"
                + "  int32 NUM = 1;\n"
                + "  string STREET = 2;\n"
                + "}");
        srClient.register("test", schema);
        ResolvedSchema flinkSchema = client.getSchema(1).resolve(manager.getSchemaResolver());
        assertThat(flinkSchema.getColumns().size()).isEqualTo(2);
        assertThat(flinkSchema.getColumns().get(0).getName()).isEqualTo("NUM");
        assertThat(flinkSchema.getColumns().get(0).getDataType()).isEqualTo(DataTypes.INT());
        assertThat(flinkSchema.getColumns().get(1).getName()).isEqualTo("STREET");
        assertThat(flinkSchema.getColumns().get(1).getDataType()).isEqualTo(DataTypes.STRING());
    }
}
