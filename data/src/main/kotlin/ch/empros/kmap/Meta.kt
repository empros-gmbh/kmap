package ch.empros.kmap

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import kotlin.reflect.KClass

typealias ColumnName = String

data class ColumnLabel(private val value: String) : CharSequence by value {
  override fun toString() = value
}

/**
 * Describes a column's metadata.
 * Note: The distinction between [name] and [label] is important:
 * According to JDBC, the name represents the column name in the underlying table while the label
 * corresponds to a possible column alias used in the query.
 */
sealed class KmColumn(open val name: ColumnName,
                      open val label: ColumnName) {
  abstract fun getVmType(): KClass<*>
}

data class DateColumn(override val name: String, override val label: String) : KmColumn(name, label) {
  override fun getVmType() = Date::class
}

data class DateTimeColumn(override val name: String, override val label: String) : KmColumn(name, label) {
  override fun getVmType() = Timestamp::class
}

data class StringColumn(override val name: ColumnName,
                        override val label: ColumnName = name,
                        val length: Int) : KmColumn(name, label) {
  override fun getVmType() = String::class
}

data class ClobColumn(override val name: ColumnName,
                      override val label: ColumnName = name) : KmColumn(name, label) {
  override fun getVmType(): KClass<String> = String::class
}

data class BooleanColumn(override val name: ColumnName,
                         override val label: ColumnName = name) : KmColumn(name, label) {
  override fun getVmType() = Boolean::class
}

data class IntColumn(override val name: ColumnName,
                     override val label: ColumnName = name) : KmColumn(name, label) {
  override fun getVmType() = Int::class
}

data class DoubleColumn(override val name: String, override val label: String) : KmColumn(name, label) {
  override fun getVmType(): KClass<*> = Double::class
}

data class LongColumn(override val name: String, override val label: String) : KmColumn(name, label) {
  override fun getVmType(): KClass<*> = Long::class
}

data class BigDecimalColumn(override val name: String, override val label: String) : KmColumn(name, label) {
  override fun getVmType(): KClass<*> = BigDecimal::class
}

data class FloatColumn(override val name: ColumnName, override val label: ColumnName) : KmColumn(name, label) {
  override fun getVmType(): KClass<*> = Float::class
}

class MetaData(cols: List<KmColumn>) {

  private val columns = cols.toList() // since cols could be a mutable list, we make a copy
  private val label2Index = columns.mapIndexed { i, column -> column.label to i }.toMap()
  private val name2Column = columns.associateBy { it.name }

  val colCount = columns.size

  fun <R> map(transform: (KmColumn) -> R): List<R> {
    return columns.map(transform)
  }

  fun forEach(action: (KmColumn) -> Unit) {
    columns.forEach(action)
  }

  fun indexOf(label: String): Int = label2Index.getOrElse(label) { throw IllegalArgumentException("Unknown label: $label") }

  fun byName(name: ColumnName): KmColumn = name2Column.getOrElse(name) { throw IllegalArgumentException("Unknown name: $name") }

  operator fun get(index: Int): KmColumn = columns[index]

  operator fun get(label: String): KmColumn = get(indexOf(label))

}

