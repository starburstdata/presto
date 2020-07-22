/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.kafka.TestingKafka;
import io.prestosql.tpch.TpchTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.TestngUtils.toDataProvider;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class TestKafkaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private TestingKafka testingKafka;
    private String rawFormatTopic;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = new TestingKafka();
        rawFormatTopic = "test_raw_" + UUID.randomUUID().toString().replaceAll("-", "_");
        Map<SchemaTableName, KafkaTopicDescription> extraTopicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                .put(new SchemaTableName("default", rawFormatTopic),
                        createDescription(rawFormatTopic, "default", rawFormatTopic,
                                createFieldGroup("raw", ImmutableList.of(
                                        createOneFieldDescription("bigint_long", BIGINT, "0", "LONG"),
                                        createOneFieldDescription("bigint_int", BIGINT, "8", "INT"),
                                        createOneFieldDescription("bigint_short", BIGINT, "12", "SHORT"),
                                        createOneFieldDescription("bigint_byte", BIGINT, "14", "BYTE"),
                                        createOneFieldDescription("double_double", DOUBLE, "15", "DOUBLE"),
                                        createOneFieldDescription("double_float", DOUBLE, "23", "FLOAT"),
                                        createOneFieldDescription("varchar_byte", createVarcharType(6), "27:33", "BYTE"),
                                        createOneFieldDescription("boolean_long", BOOLEAN, "33", "LONG"),
                                        createOneFieldDescription("boolean_int", BOOLEAN, "41", "INT"),
                                        createOneFieldDescription("boolean_short", BOOLEAN, "45", "SHORT"),
                                        createOneFieldDescription("boolean_byte", BOOLEAN, "47", "BYTE")))))
                .build();

        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setTables(TpchTable.getTables())
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .putAll(extraTopicDescriptions)
                        .build())
                .build();

        testingKafka.createTopic(rawFormatTopic);

        return queryRunner;
    }

    @Test
    public void testColumnReferencedTwice()
    {
        ByteBuffer buf = ByteBuffer.allocate(48);
        buf.putLong(1234567890123L); // 0-8
        buf.putInt(123456789); // 8-12
        buf.putShort((short) 12345); // 12-14
        buf.put((byte) 127); // 14
        buf.putDouble(123456789.123); // 15-23
        buf.putFloat(123456.789f); // 23-27
        buf.put("abcdef".getBytes(UTF_8)); // 27-33
        buf.putLong(1234567890123L); // 33-41
        buf.putInt(123456789); // 41-45
        buf.putShort((short) 12345); // 45-47
        buf.put((byte) 127); // 47

        insertData(rawFormatTopic, buf.array());

        assertQuery("SELECT " +
                          "bigint_long, bigint_int, bigint_short, bigint_byte, " +
                          "double_double, double_float, varchar_byte, " +
                          "boolean_long, boolean_int, boolean_short, boolean_byte " +
                        "FROM default." + rawFormatTopic + " WHERE " +
                          "bigint_long = 1234567890123 AND bigint_int = 123456789 AND bigint_short = 12345 AND bigint_byte = 127 AND " +
                          "double_double = 123456789.123 AND double_float != 1.0 AND varchar_byte = 'abcdef' AND " +
                          "boolean_long = TRUE AND boolean_int = TRUE AND boolean_short = TRUE AND boolean_byte = TRUE",
                "VALUES (1234567890123, 123456789, 12345, 127, 123456789.123, 123456.789, 'abcdef', TRUE, TRUE, TRUE, TRUE)");
        assertQuery("SELECT " +
                          "bigint_long, bigint_int, bigint_short, bigint_byte, " +
                          "double_double, double_float, varchar_byte, " +
                          "boolean_long, boolean_int, boolean_short, boolean_byte " +
                        "FROM default." + rawFormatTopic + " WHERE " +
                          "bigint_long < 1234567890124 AND bigint_int < 123456790 AND bigint_short < 12346 AND bigint_byte < 128 AND " +
                          "double_double < 123456789.124 AND double_float > 2 AND varchar_byte <= 'abcdef' AND " +
                          "boolean_long != FALSE AND boolean_int != FALSE AND boolean_short != FALSE AND boolean_byte != FALSE",
                "VALUES (1234567890123, 123456789, 12345, 127, 123456789.123, 123456.789, 'abcdef', TRUE, TRUE, TRUE, TRUE)");
    }

    private void insertData(String topic, byte[] data)
    {
        try (KafkaProducer<byte[], byte[]> producer = createProducer()) {
            producer.send(new ProducerRecord<>(topic, data));
            producer.flush();
        }
    }

    private KafkaProducer<byte[], byte[]> createProducer()
    {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, testingKafka.getConnectString());
        properties.put(ACKS_CONFIG, "all");
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    @Test
    public void testReadAllDataTypes()
    {
        String json = "{" +
                "\"j_varchar\"                              : \"ala ma kota\"                    ," +
                "\"j_bigint\"                               : \"9223372036854775807\"            ," +
                "\"j_integer\"                              : \"2147483647\"                     ," +
                "\"j_smallint\"                             : \"32767\"                          ," +
                "\"j_tinyint\"                              : \"127\"                            ," +
                "\"j_double\"                               : \"1234567890.123456789\"           ," +
                "\"j_boolean\"                              : \"true\"                           ," +
                "\"j_timestamp_milliseconds_since_epoch\"   : \"1518182116000\"                  ," +
                "\"j_timestamp_seconds_since_epoch\"        : \"1518182117\"                     ," +
                "\"j_timestamp_iso8601\"                    : \"2018-02-09T13:15:18\"            ," +
                "\"j_timestamp_rfc2822\"                    : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                "\"j_timestamp_custom\"                     : \"02/2018/09 13:15:20\"            ," +
                "\"j_date_iso8601\"                         : \"2018-02-11\"                     ," +
                "\"j_date_rfc2822\"                         : \"Mon Feb 12 13:15:16 Z 2018\"     ," +
                "\"j_date_custom\"                          : \"2018/13/02\"                     ," +
                "\"j_time_milliseconds_since_epoch\"        : \"47716000\"                       ," +
                "\"j_time_seconds_since_epoch\"             : \"47717\"                          ," +
                "\"j_time_iso8601\"                         : \"13:15:18\"                       ," +
                "\"j_time_rfc2822\"                         : \"Thu Jan 01 13:15:19 Z 1970\"     ," +
                "\"j_time_custom\"                          : \"15:13:20\"                       ," +
                "\"j_timestamptz_milliseconds_since_epoch\" : \"1518182116000\"                  ," +
                "\"j_timestamptz_seconds_since_epoch\"      : \"1518182117\"                     ," +
                "\"j_timestamptz_iso8601\"                  : \"2018-02-09T13:15:18Z\"           ," +
                "\"j_timestamptz_rfc2822\"                  : \"Fri Feb 09 13:15:19 Z 2018\"     ," +
                "\"j_timestamptz_custom\"                   : \"02/2018/09 13:15:20\"            ," +
                "\"j_timetz_milliseconds_since_epoch\"      : \"47716000\"                       ," +
                "\"j_timetz_seconds_since_epoch\"           : \"47717\"                          ," +
                "\"j_timetz_iso8601\"                       : \"13:15:18Z\"                      ," +
                "\"j_timetz_rfc2822\"                       : \"Thu Jan 01 13:15:19 Z 1970\"     ," +
                "\"j_timetz_custom\"                        : \"15:13:20\"                       }";

        insertData("read_test.all_datatypes_json", json.getBytes(UTF_8));
        assertQuery(
                "SELECT " +
                        "  c_varchar " +
                        ", c_bigint " +
                        ", c_integer " +
                        ", c_smallint " +
                        ", c_tinyint " +
                        ", c_double " +
                        ", c_boolean " +
                        ", c_timestamp_milliseconds_since_epoch " +
                        ", c_timestamp_seconds_since_epoch " +
                        ", c_timestamp_iso8601 " +
                        ", c_timestamp_rfc2822 " +
                        ", c_timestamp_custom " +
                        ", c_date_iso8601 " +
                        ", c_date_rfc2822 " +
                        ", c_date_custom " +
                        ", c_time_milliseconds_since_epoch " +
                        ", c_time_seconds_since_epoch " +
                        ", c_time_iso8601 " +
                        ", c_time_rfc2822 " +
                        ", c_time_custom " +
                        // H2 does not support TIMESTAMP WITH TIME ZONE so cast to VARCHAR
                        ", cast(c_timestamptz_milliseconds_since_epoch as VARCHAR) " +
                        ", cast(c_timestamptz_seconds_since_epoch as VARCHAR) " +
                        ", cast(c_timestamptz_iso8601 as VARCHAR) " +
                        ", cast(c_timestamptz_rfc2822 as VARCHAR) " +
                        ", cast(c_timestamptz_custom as VARCHAR) " +
                        // H2 does not support TIME WITH TIME ZONE so cast to VARCHAR
                        ", cast(c_timetz_milliseconds_since_epoch as VARCHAR) " +
                        ", cast(c_timetz_seconds_since_epoch as VARCHAR) " +
                        ", cast(c_timetz_iso8601 as VARCHAR) " +
                        ", cast(c_timetz_rfc2822 as VARCHAR) " +
                        ", cast(c_timetz_custom as VARCHAR) " +
                        "FROM read_test.all_datatypes_json ",
                "VALUES (" +
                        "  'ala ma kota'" +
                        ", 9223372036854775807" +
                        ", 2147483647" +
                        ", 32767" +
                        ", 127" +
                        ", 1234567890.123456789" +
                        ", true" +
                        ", TIMESTAMP '2018-02-09 13:15:16'" +
                        ", TIMESTAMP '2018-02-09 13:15:17'" +
                        ", TIMESTAMP '2018-02-09 13:15:18'" +
                        ", TIMESTAMP '2018-02-09 13:15:19'" +
                        ", TIMESTAMP '2018-02-09 13:15:20'" +
                        ", DATE '2018-02-11'" +
                        ", DATE '2018-02-12'" +
                        ", DATE '2018-02-13'" +
                        ", TIME '13:15:16'" +
                        ", TIME '13:15:17'" +
                        ", TIME '13:15:18'" +
                        ", TIME '13:15:19'" +
                        ", TIME '13:15:20'" +
                        ", '2018-02-09 13:15:16.000 UTC'" +
                        ", '2018-02-09 13:15:17.000 UTC'" +
                        ", '2018-02-09 13:15:18.000 UTC'" +
                        ", '2018-02-09 13:15:19.000 UTC'" +
                        ", '2018-02-09 13:15:20.000 UTC'" +
                        ", '13:15:16.000 UTC'" +
                        ", '13:15:17.000 UTC'" +
                        ", '13:15:18.000 UTC'" +
                        ", '13:15:19.000 UTC'" +
                        ", '13:15:20.000 UTC'" +
                        ")");
    }

    private KafkaTopicDescription createDescription(String name, String schema, String topic, Optional<KafkaTopicFieldGroup> message)
    {
        return new KafkaTopicDescription(name, Optional.of(schema), topic, Optional.empty(), message);
    }

    private Optional<KafkaTopicFieldGroup> createFieldGroup(String dataFormat, List<KafkaTopicFieldDescription> fields)
    {
        return Optional.of(new KafkaTopicFieldGroup(dataFormat, Optional.empty(), fields));
    }

    private KafkaTopicFieldDescription createOneFieldDescription(String name, Type type, String mapping, String dataFormat)
    {
        return new KafkaTopicFieldDescription(name, type, mapping, null, dataFormat, null, false);
    }

    @Test(dataProvider = "roundTripAllFormatsDataProvider")
    public void testRoundTripAllFormats(RoundTripTestCase testCase)
    {
        assertUpdate("INSERT into write_test." + testCase.getTableName() +
                " (" + testCase.getFieldNames() + ")" +
                " VALUES " + testCase.getRowValues(), testCase.getNumRows());
        assertQuery("SELECT " + testCase.getFieldNames() + " FROM write_test." + testCase.getTableName() +
                " WHERE f_bigint > 1",
                "VALUES " + testCase.getRowValues());
    }

    @DataProvider
    public final Object[][] roundTripAllFormatsDataProvider()
    {
        return roundTripAllFormatsData().stream()
                .collect(toDataProvider());
    }

    private List<RoundTripTestCase> roundTripAllFormatsData()
    {
        return ImmutableList.<RoundTripTestCase>builder()
                .add(new RoundTripTestCase(
                        "all_datatypes_avro",
                        ImmutableList.of("f_bigint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_csv",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123456, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .add(new RoundTripTestCase(
                        "all_datatypes_raw",
                        ImmutableList.of("kafka_key", "f_varchar", "f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean"),
                        ImmutableList.of(
                                ImmutableList.of(1, "'test'", 100000, 1000, 100, 10, 1000.001, true),
                                ImmutableList.of(1, "'abcd'", 123456, 1234, 123, 12, 12345.123, false))))
                .add(new RoundTripTestCase(
                        "all_datatypes_json",
                        ImmutableList.of("f_bigint", "f_int", "f_smallint", "f_tinyint", "f_double", "f_boolean", "f_varchar"),
                        ImmutableList.of(
                                ImmutableList.of(100000, 1000, 100, 10, 1000.001, true, "'test'"),
                                ImmutableList.of(123748, 1234, 123, 12, 12345.123, false, "'abcd'"))))
                .build();
    }

    private static final class RoundTripTestCase
    {
        private final String tableName;
        private final List<String> fieldNames;
        private final List<List<Object>> rowValues;
        private final int numRows;

        public RoundTripTestCase(String tableName, List<String> fieldNames, List<List<Object>> rowValues)
        {
            for (List<Object> row : rowValues) {
                checkArgument(fieldNames.size() == row.size(), "sizes of fieldNames and rowValues are not equal");
            }
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.rowValues = ImmutableList.copyOf(rowValues);
            this.numRows = this.rowValues.size();
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getFieldNames()
        {
            return String.join(", ", fieldNames);
        }

        public String getRowValues()
        {
            String[] rows = new String[numRows];
            for (int i = 0; i < numRows; i++) {
                rows[i] = rowValues.get(i).stream().map(Object::toString).collect(Collectors.joining(", ", "(", ")"));
            }
            return String.join(", ", rows);
        }

        public int getNumRows()
        {
            return numRows;
        }

        @Override
        public String toString()
        {
            return tableName; // for test case label in IDE
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (testingKafka != null) {
            testingKafka.close();
            testingKafka = null;
        }
    }
}