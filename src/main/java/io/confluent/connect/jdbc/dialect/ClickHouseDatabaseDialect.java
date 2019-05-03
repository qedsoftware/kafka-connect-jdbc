package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for ClickHouse.
 */
public class ClickHouseDatabaseDialect extends GenericDatabaseDialect {
  /**
   * The provider for {@link MySqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(ClickHouseDatabaseDialect.class.getSimpleName(), "clickhouse");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new ClickHouseDatabaseDialect(config);
    }
  }

  TimeZone timeZone;
  String engine;
  String partitionExpression;
  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public ClickHouseDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
    
    if (config instanceof JdbcSourceConnectorConfig) {
        timeZone = ((JdbcSourceConnectorConfig) config).timeZone();
      } else if (config instanceof JdbcSinkConfig) {
        timeZone = ((JdbcSinkConfig) config).timeZone;
      } else {
        timeZone = TimeZone.getTimeZone(ZoneOffset.UTC);
      }
    
    if (ConfigDef.Type.STRING == config.typeOf("table.engine")) {
    	engine = config.getString("table.engine");
    }
    else {
    	engine = "MergeTree";
    }

    if (ConfigDef.Type.STRING == config.typeOf("table.partition")) {
    	partitionExpression = config.getString("table.partition");
    }
    

  }
  

  @Override
  public String buildCreateTableStatement(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    builder.append(")");

    builder.append(System.lineSeparator());
    builder.append("ENGINE = ");
    builder.append(engine);
    
    if (partitionExpression != null) {
        builder.append(System.lineSeparator());
    	builder.append("PARTITION BY ");
    	builder.append(partitionExpression);
    }

    if (!pkFieldNames.isEmpty()) {
        builder.append(System.lineSeparator());
        builder.append("ORDER BY (");
        builder.appendList()
               .delimitedBy(",")
               .transformedBy(ExpressionBuilder.quote())
               .of(pkFieldNames);
        builder.append(")");
      }

    return builder.toString();
  }
  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIMESTAMP_FORMATS =
      ThreadLocal.withInitial(HashMap::new);


  public static String formatTimestamp(java.util.Date date, TimeZone timeZone) {
    return TIMEZONE_TIMESTAMP_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  private static final ThreadLocal<Map<TimeZone, SimpleDateFormat>> TIMEZONE_TIME_FORMATS =
      ThreadLocal.withInitial(HashMap::new);


  public static String formatTime(java.util.Date date, TimeZone timeZone) {
    return TIMEZONE_TIME_FORMATS.get().computeIfAbsent(timeZone, aTimeZone -> {
      SimpleDateFormat sdf = new SimpleDateFormat("0000-00-00 HH:mm:ss");
      sdf.setTimeZone(aTimeZone);
      return sdf;
    }).format(date);
  }

  
  protected void formatColumnValue(
	      ExpressionBuilder builder,
	      String schemaName,
	      Map<String, String> schemaParameters,
	      Schema.Type type,
	      Object value
	  ) {
	    if (schemaName != null) {
	      switch (schemaName) {
	        case Date.LOGICAL_NAME:
	          builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone));
	          return;
	        case Time.LOGICAL_NAME:
	          builder.appendStringQuoted(formatTime((java.util.Date) value, timeZone));
	          return;
	        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
	          builder.appendStringQuoted(formatTimestamp((java.util.Date) value, timeZone)
	          );
	          return;
	        default:
	          // fall through to regular types
	          break;
	      }
	    }
	    super.formatColumnValue(builder, schemaName, schemaParameters, type, value);
  }
  
  protected void writeColumnSpec(
	      ExpressionBuilder builder,
	      SinkRecordField f
	  ) {
	    builder.appendColumnName(f.name());
	    builder.append(" ");
	    String sqlType = getSqlType(f);
	    
	    if (isColumnOptional(f)) {
	    	builder.append("Nullable(");
	    }
	    
	    builder.append(sqlType);
	    
	    if (isColumnOptional(f)) {
	    	builder.append(")");
	    }
	    
	    if (f.defaultValue() != null) {
	    	
	      builder.append(" DEFAULT ");
	      String type = getSqlType(f);
	      builder.append("to" + type + "(");
	      Object defaultValue = f.defaultValue();
	      
	      formatColumnValue(
	          builder,
	          f.schemaName(),
	          f.schemaParameters(),
	          f.schemaType(),
	          defaultValue
	      );
	      builder.append(")");

	    }
	  }


  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
          // TODO - which Decimal ? Decimal 32/64/128
          return "Decimal64(" + scale + ")";
        case Date.LOGICAL_NAME:
            return "Date";
        case Time.LOGICAL_NAME:
            return "DateTime"; // date parts will be 00's
        case Timestamp.LOGICAL_NAME:
          return "DateTime";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "Int8";
      case INT16:
        return "Int16";
      case INT32:
        return "Int32";
      case INT64:
        return "Int64";
      case FLOAT32:
        return "Float32";
      case FLOAT64:
        return "Float64";
      case BOOLEAN:
        return "UInt8";
      case STRING:
        return "String";
      default:
        return super.getSqlType(field);
    }
  }
  

  protected boolean maybeBindLogical(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Date.LOGICAL_NAME:

          statement.setString(
              index,
              DateTimeUtils.formatDate((java.util.Date) value, timeZone)
          );
          return true;
        case Decimal.LOGICAL_NAME:
          statement.setBigDecimal(index, (BigDecimal) value);
          return true;
        case Time.LOGICAL_NAME:
          statement.setString(
              index,
              formatTime((java.util.Date) value, timeZone)
          );
          return true;
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          statement.setString(
              index,
              formatTimestamp((java.util.Date) value, timeZone)
          );
          return true;
        default:
          return false;
      }
    }
    return false;
  }


  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    final boolean newlines = fields.size() > 1;

    final Transform<SinkRecordField> transform = (builder, field) -> {
      if (newlines) {
        builder.appendNewLine();
      }
      builder.append("ADD COLUMN ");
      writeColumnSpec(builder, field);
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(transform)
           .of(fields);
    return Collections.singletonList(builder.toString());
  }

}
