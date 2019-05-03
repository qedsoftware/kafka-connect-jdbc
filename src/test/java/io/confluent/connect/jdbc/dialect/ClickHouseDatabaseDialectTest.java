package io.confluent.connect.jdbc.dialect;


import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class ClickHouseDatabaseDialectTest extends BaseDialectTest<ClickHouseDatabaseDialect> {

  @Override
  protected ClickHouseDatabaseDialect createDialect() {
    return new ClickHouseDatabaseDialect(sourceConfigWithUrl("jdbc:ClickHouse://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "Int8");
    assertPrimitiveMapping(Type.INT16, "Int16");
    assertPrimitiveMapping(Type.INT32, "Int32");
    assertPrimitiveMapping(Type.INT64, "Int64");
    assertPrimitiveMapping(Type.FLOAT32, "Float32");
    assertPrimitiveMapping(Type.FLOAT64, "Float64");
    assertPrimitiveMapping(Type.BOOLEAN, "UInt8");
    assertPrimitiveMapping(Type.STRING, "String");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "Decimal64(0)");
    assertDecimalMapping(3, "Decimal64(3)");
    assertDecimalMapping(4, "Decimal64(4)");
    assertDecimalMapping(5, "Decimal64(5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("Int8", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("Int16", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("Int32", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("Int64", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("Float32", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("Float64", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("UInt8", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("String", Schema.STRING_SCHEMA);
//    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("Decimal64(0)", Decimal.schema(0));
    verifyDataTypeMapping("Decimal64(2)", Decimal.schema(2));
    verifyDataTypeMapping("Date", Date.SCHEMA);
    verifyDataTypeMapping("DateTime", Time.SCHEMA);
    verifyDataTypeMapping("DateTime", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("Date");
  }

  @Test
  public void shouldMapTimeSchemaTypeToDateTimeSqlType() {
    assertTimeMapping("DateTime");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("DateTime");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE \"myTable\" (\n" + 
		"\"c1\" Int32,\n" + 
		"\"c2\" Int64,\n" +
        "\"c3\" String,\n" + 
		"\"c4\" Nullable(String),\n" +
        "\"c5\" Nullable(Date) DEFAULT toDate('2001-03-15'),\n" + 
        "\"c6\" Nullable(DateTime) DEFAULT toDateTime('0000-00-00 00:00:00'),\n" +
        "\"c7\" Nullable(DateTime) DEFAULT toDateTime('2001-03-15 00:00:00'),\n" + 
        "\"c8\" Nullable(Decimal64(4)))\n" +
        "ENGINE = MergeTree\n" +
        "ORDER BY (\"c1\")";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {
        "ALTER TABLE \"myTable\" \n" + 
		"ADD COLUMN \"c1\" Int32,\n" + 
		"ADD COLUMN \"c2\" Int64,\n" +
        "ADD COLUMN \"c3\" String,\n" + 
		"ADD COLUMN \"c4\" Nullable(String),\n" +
        "ADD COLUMN \"c5\" Nullable(Date) DEFAULT toDate('2001-03-15'),\n" + 
		"ADD COLUMN \"c6\" Nullable(DateTime) DEFAULT toDateTime('0000-00-00 00:00:00'),\n" +
        "ADD COLUMN \"c7\" Nullable(DateTime) DEFAULT toDateTime('2001-03-15 00:00:00'),\n" +
        "ADD COLUMN \"c8\" Nullable(Decimal64(4))"};
    assertStatements(sql, statements);
  }


  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" Int32)" + 
    		System.lineSeparator() + "ENGINE = MergeTree");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateOneColNoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "col1 Int32)" + 
    System.lineSeparator() + "ENGINE = MergeTree");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" Int32)" +
        System.lineSeparator() + "ENGINE = MergeTree" + System.lineSeparator() + "ORDER BY (\"pk1\")");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" Int32," +
        System.lineSeparator() + "\"pk2\" Int32," + System.lineSeparator() +
        "\"col1\" Int32)" + System.lineSeparator() + "ENGINE = MergeTree" + System.lineSeparator() + "ORDER BY (\"pk1\",\"pk2\")");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 Int32," +
        System.lineSeparator() + "pk2 Int32," + System.lineSeparator() +
        "col1 Int32)" + System.lineSeparator() + "ENGINE = MergeTree" + System.lineSeparator() + "ORDER BY (pk1,pk2)");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD COLUMN \"newcol1\" Nullable(Int32)");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD COLUMN \"newcol1\" Nullable(Int32)," +
        System.lineSeparator() + "ADD COLUMN \"newcol2\" Int32 DEFAULT toInt32(42)");
  }

  @Test
  public void insert() {
    TableId customers = tableId("customers");
    String expected = "INSERT INTO \"customers\"(\"age\",\"firstName\",\"lastName\") VALUES(?,?,?)";
    String sql = dialect.buildInsertStatement(customers, columns(customers),
                                              columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }

  @Test
  public void update() {
    TableId customers = tableId("customers");
    String expected =
        "UPDATE \"customers\" SET \"age\" = ?, \"firstName\" = ?, \"lastName\" = ? WHERE " + "\"id\" = ?";
    String sql = dialect.buildUpdateStatement(customers, columns(customers, "id"),
                                              columns(customers, "age", "firstName", "lastName"));
    assertEquals(expected, sql);
  }



  @Test
  public void bindFieldPrimitiveValues() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.INT8_SCHEMA, (byte) 42).setByte(index, (byte) 42);
    verifyBindField(++index, Schema.INT16_SCHEMA, (short) 42).setShort(index, (short) 42);
    verifyBindField(++index, Schema.INT32_SCHEMA, 42).setInt(index, 42);
    verifyBindField(++index, Schema.INT64_SCHEMA, 42L).setLong(index, 42L);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, false).setBoolean(index, false);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, true).setBoolean(index, true);
    verifyBindField(++index, Schema.FLOAT32_SCHEMA, -42f).setFloat(index, -42f);
    verifyBindField(++index, Schema.FLOAT64_SCHEMA, 42d).setDouble(index, 42d);
    verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
    verifyBindField(
        ++index,
        Decimal.schema(0),
        new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN)
    ).setBigDecimal(index, new BigDecimal(2));
    Calendar utcCalendar = DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC));
    verifyBindField(
      ++index,
      Date.SCHEMA,
      new java.util.Date(0)
    ).setString(index, "1970-01-01");
    verifyBindField(
      ++index,
      Time.SCHEMA,
      new java.util.Date(1000)
    ).setString(index, "0000-00-00 00:00:01");
    verifyBindField(
      ++index,
      Timestamp.SCHEMA,
      new java.util.Date(100)
    ).setString(index,  "1970-01-01 00:00:00");
  }
}