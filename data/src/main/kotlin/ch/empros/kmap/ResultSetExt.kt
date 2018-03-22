package ch.empros.kmap

import java.io.Reader
import java.sql.ResultSet
import java.sql.Types

/**
 * Extension function that maps this [ResultSet]'s JDBC-based meta data to a kMap meta data object.
 */
fun ResultSet.kMapMetaData(): MetaData =
  MetaData(
    IntRange(1, metaData.columnCount)
      .map { i ->
        with(metaData) {
          val name = getColumnName(i)
          val label = getColumnLabel(i)
          val ctype = getColumnType(i)
          when (ctype) {
            Types.BOOLEAN -> BooleanColumn(name, label)
            Types.CLOB -> ClobColumn(name, label)
            Types.DATE -> DateColumn(name, label)
            Types.FLOAT, Types.REAL -> FloatColumn(name, label)
            Types.DOUBLE -> DoubleColumn(name, label)
            Types.INTEGER -> IntColumn(name, label)
            Types.BIGINT -> LongColumn(name, label)
            Types.NUMERIC, Types.DECIMAL -> BigDecimalColumn(name, label)
            Types.TIMESTAMP -> DateTimeColumn(name, label)
            Types.VARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.LONGVARCHAR -> StringColumn(name, label, getColumnDisplaySize(i))
            else -> throw IllegalArgumentException("KMap ResultSet Mapping: Unsupported SQL-Type '$ctype'")
          }
        }
      }
  )

/**
 * Extension function that reads this [ResultSet]'s records and puts them into a [Page] object.
 * Upon completion this [ResultSet] is closed.
 * TODO This method is wrong! We may not close the ResultSet automatically, we need to iterate lazily.
 */
fun ResultSet.toPageThenClose(): Page {
  if (isClosed) throw IllegalStateException("Unable to read data from closed resultSet")
  val records = mutableListOf<Record>()
  val metaData = kMapMetaData()
  while (next()) {
    records.add(currentRecord(metaData))
  }
  close()
  return Page(metaData, records)
}

/**
 * Extension function that creates a KMap [Record] for the current entry on this [ResultSet].
 * Usually you do not need to call this method directly and rather use [toPageThenClose] to get this [ResultSet]'s data.
 *
 * But if you think you absolutely must use this method, read on:
 * If you call this method repeatedly on the same [ResultSet] instance, you can prevent the repeated construction of a
 * [kMapMetaData] object by calling [ResultSet.kMapMetaData()] beforehand and pass its result to this method,
 * see [toPageThenClose] for an example.
 *
 * It is possible to call this method with a [MetaData] instance that was created on another [ResultSet] instance.
 * The implementation does not take any measures to prevent you from doing such a stupid thing, and you will likely get an
 * exception as a result. So heed your doctor's advice: if it hurts, don't do it.
 */
fun ResultSet.currentRecord(metaData: MetaData = kMapMetaData()): Record {

  val values = IntRange(0, metaData.colCount - 1).map { index ->
    val kmColumn = metaData[index]
    val value: Any? = when (kmColumn) {
      is BooleanColumn -> this.getBoolean(kmColumn.label)
      is BigDecimalColumn -> this.getBigDecimal(kmColumn.label)
      is DateColumn -> this.getDate(kmColumn.label)
      is DateTimeColumn -> this.getTimestamp(kmColumn.label)
      is DoubleColumn -> this.getDouble(kmColumn.label)
      is FloatColumn -> this.getFloat(kmColumn.label)
      is LongColumn -> this.getLong(kmColumn.label)
      is IntColumn -> this.getInt(kmColumn.label)
      is StringColumn -> this.getString(kmColumn.label)
      is ClobColumn -> this.getClob(kmColumn.label).characterStream.readText()
    }
    value
  }.toList()

  return Record(metaData, values)
}
