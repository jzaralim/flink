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
                + "      \"name\": \"BOOL\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"boolean\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"STR\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"string\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"B\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"bytes\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"I\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"int\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"BIG\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"long\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"DOU\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        \"double\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"DEC\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"logicalType\": \"decimal\",\n"
                + "          \"precision\": 4,\n"
                + "          \"scale\": 2,\n"
                + "          \"type\": \"bytes\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"T\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"logicalType\": \"time-millis\",\n"
                + "          \"type\": \"int\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"D\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"logicalType\": \"date\",\n"
                + "          \"type\": \"int\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"TS\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"logicalType\": \"timestamp-millis\",\n"
                + "          \"type\": \"long\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"A\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"items\": [\n"
                + "            \"null\",\n"
                + "            \"string\"\n"
                + "          ],\n"
                + "          \"type\": \"array\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"S\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"fields\": [\n"
                + "            {\n"
                + "              \"default\": null,\n"
                + "              \"name\": \"NAME\",\n"
                + "              \"type\": [\n"
                + "                \"null\",\n"
                + "                \"string\"\n"
                + "              ]\n"
                + "            },\n"
                + "            {\n"
                + "              \"default\": null,\n"
                + "              \"name\": \"AGE\",\n"
                + "              \"type\": [\n"
                + "                \"null\",\n"
                + "                \"int\"\n"
                + "              ]\n"
                + "            }\n"
                + "          ],\n"
                + "          \"name\": \"KsqlDataSourceSchema_S\",\n"
                + "          \"type\": \"record\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"default\": null,\n"
                + "      \"name\": \"M\",\n"
                + "      \"type\": [\n"
                + "        \"null\",\n"
                + "        {\n"
                + "          \"items\": {\n"
                + "            \"connect.internal.type\": \"MapEntry\",\n"
                + "            \"fields\": [\n"
                + "              {\n"
                + "                \"default\": null,\n"
                + "                \"name\": \"key\",\n"
                + "                \"type\": [\n"
                + "                  \"null\",\n"
                + "                  \"string\"\n"
                + "                ]\n"
                + "              },\n"
                + "              {\n"
                + "                \"default\": null,\n"
                + "                \"name\": \"value\",\n"
                + "                \"type\": [\n"
                + "                  \"null\",\n"
                + "                  \"int\"\n"
                + "                ]\n"
                + "              }\n"
                + "            ],\n"
                + "            \"name\": \"KsqlDataSourceSchema_M\",\n"
                + "            \"type\": \"record\"\n"
                + "          },\n"
                + "          \"type\": \"array\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ],\n"
                + "  \"name\": \"KsqlDataSourceSchema\",\n"
                + "  \"namespace\": \"io.confluent.ksql.avro_schemas\",\n"
                + "  \"type\": \"record\"\n"
                + "}");
        srClient.register("test", schema, 1,1);
        ResolvedSchema flinkSchema = client.getSchema(1).resolve(manager.getSchemaResolver());
        verifySchema(flinkSchema);
    }

    @Test
    public void testJsonSchemaTranslation() throws IOException, RestClientException {
        JsonSchema schema = new JsonSchema("{\n"
                + "  \"properties\": {\n"
                + "    \"A\": {\n"
                + "      \"connect.index\": 10,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"items\": {\n"
                + "            \"oneOf\": [\n"
                + "              {\n"
                + "                \"type\": \"null\"\n"
                + "              },\n"
                + "              {\n"
                + "                \"type\": \"string\"\n"
                + "              }\n"
                + "            ]\n"
                + "          },\n"
                + "          \"type\": \"array\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"B\": {\n"
                + "      \"connect.index\": 2,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"bytes\",\n"
                + "          \"type\": \"string\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"BIG\": {\n"
                + "      \"connect.index\": 4,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"int64\",\n"
                + "          \"type\": \"integer\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"BOOL\": {\n"
                + "      \"connect.index\": 0,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"type\": \"boolean\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"D\": {\n"
                + "      \"connect.index\": 8,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"int32\",\n"
                + "          \"connect.version\": 1,\n"
                + "          \"title\": \"org.apache.kafka.connect.data.Date\",\n"
                + "          \"type\": \"integer\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"DEC\": {\n"
                + "      \"connect.index\": 6,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.parameters\": {\n"
                + "            \"connect.decimal.precision\": \"4\",\n"
                + "            \"scale\": \"2\"\n"
                + "          },\n"
                + "          \"connect.type\": \"bytes\",\n"
                + "          \"connect.version\": 1,\n"
                + "          \"title\": \"org.apache.kafka.connect.data.Decimal\",\n"
                + "          \"type\": \"number\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"DOU\": {\n"
                + "      \"connect.index\": 5,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"float64\",\n"
                + "          \"type\": \"number\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"I\": {\n"
                + "      \"connect.index\": 3,\n"
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
                + "    \"M\": {\n"
                + "      \"connect.index\": 12,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"map\",\n"
                + "          \"items\": {\n"
                + "            \"properties\": {\n"
                + "              \"key\": {\n"
                + "                \"connect.index\": 0,\n"
                + "                \"oneOf\": [\n"
                + "                  {\n"
                + "                    \"type\": \"null\"\n"
                + "                  },\n"
                + "                  {\n"
                + "                    \"type\": \"string\"\n"
                + "                  }\n"
                + "                ]\n"
                + "              },\n"
                + "              \"value\": {\n"
                + "                \"connect.index\": 1,\n"
                + "                \"oneOf\": [\n"
                + "                  {\n"
                + "                    \"type\": \"null\"\n"
                + "                  },\n"
                + "                  {\n"
                + "                    \"connect.type\": \"int32\",\n"
                + "                    \"type\": \"integer\"\n"
                + "                  }\n"
                + "                ]\n"
                + "              }\n"
                + "            },\n"
                + "            \"type\": \"object\"\n"
                + "          },\n"
                + "          \"type\": \"array\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"S\": {\n"
                + "      \"connect.index\": 11,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"properties\": {\n"
                + "            \"AGE\": {\n"
                + "              \"connect.index\": 1,\n"
                + "              \"oneOf\": [\n"
                + "                {\n"
                + "                  \"type\": \"null\"\n"
                + "                },\n"
                + "                {\n"
                + "                  \"connect.type\": \"int32\",\n"
                + "                  \"type\": \"integer\"\n"
                + "                }\n"
                + "              ]\n"
                + "            },\n"
                + "            \"NAME\": {\n"
                + "              \"connect.index\": 0,\n"
                + "              \"oneOf\": [\n"
                + "                {\n"
                + "                  \"type\": \"null\"\n"
                + "                },\n"
                + "                {\n"
                + "                  \"type\": \"string\"\n"
                + "                }\n"
                + "              ]\n"
                + "            }\n"
                + "          },\n"
                + "          \"type\": \"object\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"STR\": {\n"
                + "      \"connect.index\": 1,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"type\": \"string\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"T\": {\n"
                + "      \"connect.index\": 7,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"int32\",\n"
                + "          \"connect.version\": 1,\n"
                + "          \"title\": \"org.apache.kafka.connect.data.Time\",\n"
                + "          \"type\": \"integer\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    \"TS\": {\n"
                + "      \"connect.index\": 9,\n"
                + "      \"oneOf\": [\n"
                + "        {\n"
                + "          \"type\": \"null\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"connect.type\": \"int64\",\n"
                + "          \"connect.version\": 1,\n"
                + "          \"title\": \"org.apache.kafka.connect.data.Timestamp\",\n"
                + "          \"type\": \"integer\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  },\n"
                + "  \"type\": \"object\"\n"
                + "}");
        srClient.register("test", schema, 2,2);
        ResolvedSchema flinkSchema = client.getSchema(2).resolve(manager.getSchemaResolver());
        verifySchema(flinkSchema);
    }

    @Test
    public void testProtobufSchemaTranslation() throws IOException, RestClientException {
        ProtobufSchema schema = new ProtobufSchema("syntax = \"proto3\";\n"
                + "\n"
                + "import \"confluent/type/decimal.proto\";\n"
                + "import \"google/type/timeofday.proto\";\n"
                + "import \"google/type/date.proto\";\n"
                + "import \"google/protobuf/timestamp.proto\";\n"
                + "\n"
                + "message ConnectDefault1 {\n"
                + "  bool BOOL = 1;\n"
                + "  string STR = 2;\n"
                + "  bytes B = 3;\n"
                + "  int32 I = 4;\n"
                + "  int64 BIG = 5;\n"
                + "  double DOU = 6;\n"
                + "  confluent.type.Decimal DEC = 7 [(confluent.field_meta) = {\n"
                + "    params: [\n"
                + "      {\n"
                + "        value: \"4\",\n"
                + "        key: \"precision\"\n"
                + "      },\n"
                + "      {\n"
                + "        value: \"2\",\n"
                + "        key: \"scale\"\n"
                + "      }\n"
                + "    ]\n"
                + "  }];\n"
                + "  google.type.TimeOfDay T = 8;\n"
                + "  google.type.Date D = 9;\n"
                + "  google.protobuf.Timestamp TS = 10;\n"
                + "  repeated string A = 11;\n"
                + "  ConnectDefault2 S = 12;\n"
                + "  repeated ConnectDefault3Entry M = 13;\n"
                + "\n"
                + "  message ConnectDefault2 {\n"
                + "    string NAME = 1;\n"
                + "    int32 AGE = 2;\n"
                + "  }\n"
                + "  message ConnectDefault3Entry {\n"
                + "    string key = 1;\n"
                + "    int32 value = 2;\n"
                + "  }\n"
                + "}");
        srClient.register("test", schema, 3, 3);
        ResolvedSchema flinkSchema = client.getSchema(3).resolve(manager.getSchemaResolver());
        verifySchema(flinkSchema);
    }

    private void verifySchema(ResolvedSchema flinkSchema) {
        assertThat(flinkSchema.getColumns().size()).isEqualTo(13);
        assertThat(flinkSchema.getColumns().get(0).getName()).isEqualTo("BOOL");
        assertThat(flinkSchema.getColumns().get(0).getDataType()).isEqualTo(DataTypes.BOOLEAN());
        assertThat(flinkSchema.getColumns().get(1).getName()).isEqualTo("STR");
        assertThat(flinkSchema.getColumns().get(1).getDataType()).isEqualTo(DataTypes.STRING());
        assertThat(flinkSchema.getColumns().get(2).getName()).isEqualTo("B");
        assertThat(flinkSchema.getColumns().get(2).getDataType()).isEqualTo(DataTypes.BYTES());
        assertThat(flinkSchema.getColumns().get(3).getName()).isEqualTo("I");
        assertThat(flinkSchema.getColumns().get(3).getDataType()).isEqualTo(DataTypes.INT());
        assertThat(flinkSchema.getColumns().get(4).getName()).isEqualTo("BIG");
        assertThat(flinkSchema.getColumns().get(4).getDataType()).isEqualTo(DataTypes.BIGINT());
        assertThat(flinkSchema.getColumns().get(5).getName()).isEqualTo("DOU");
        assertThat(flinkSchema.getColumns().get(5).getDataType()).isEqualTo(DataTypes.DOUBLE());
        assertThat(flinkSchema.getColumns().get(6).getName()).isEqualTo("DEC");
        assertThat(flinkSchema.getColumns().get(6).getDataType()).isEqualTo(DataTypes.DECIMAL(4, 2));
        assertThat(flinkSchema.getColumns().get(7).getName()).isEqualTo("T");
        assertThat(flinkSchema.getColumns().get(7).getDataType()).isEqualTo(DataTypes.TIME());
        assertThat(flinkSchema.getColumns().get(8).getName()).isEqualTo("D");
        assertThat(flinkSchema.getColumns().get(8).getDataType()).isEqualTo(DataTypes.DATE());
        assertThat(flinkSchema.getColumns().get(9).getName()).isEqualTo("TS");
        assertThat(flinkSchema.getColumns().get(9).getDataType()).isEqualTo(DataTypes.TIMESTAMP());
        assertThat(flinkSchema.getColumns().get(10).getName()).isEqualTo("A");
        assertThat(flinkSchema.getColumns().get(10).getDataType()).isEqualTo(DataTypes.ARRAY(DataTypes.STRING()));
        assertThat(flinkSchema.getColumns().get(11).getName()).isEqualTo("S");
        assertThat(flinkSchema.getColumns().get(11).getDataType()).isEqualTo(DataTypes.ROW(DataTypes.FIELD("NAME", DataTypes.STRING()), DataTypes.FIELD("AGE", DataTypes.INT())).notNull());
        assertThat(flinkSchema.getColumns().get(12).getName()).isEqualTo("M");
        assertThat(flinkSchema.getColumns().get(12).getDataType()).isEqualTo(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));
    }
}
