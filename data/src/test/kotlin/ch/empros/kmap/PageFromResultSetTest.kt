package ch.empros.kmap

import ch.empros.kmap.H2MemoryDatabaseData.ACTIVE
import ch.empros.kmap.H2MemoryDatabaseData.BIRTH_DATE
import ch.empros.kmap.H2MemoryDatabaseData.DOUBLE_DISCOUNT
import ch.empros.kmap.H2MemoryDatabaseData.FIRSTNAME
import ch.empros.kmap.H2MemoryDatabaseData.FLOAT_DISCOUNT
import ch.empros.kmap.H2MemoryDatabaseData.ID
import ch.empros.kmap.H2MemoryDatabaseData.LASTNAME
import ch.empros.kmap.H2MemoryDatabaseData.SIZE
import ch.empros.kmap.H2MemoryDatabaseData.VALUATION
import ch.empros.kmap.H2MemoryDatabaseData.VAL_BIRTH_DATE
import ch.empros.kmap.H2MemoryDatabaseData.VAL_FIRSTNAME
import ch.empros.kmap.H2MemoryDatabaseData.VAL_LASTNAME
import ch.empros.kmap.H2MemoryDatabaseData.VAL_SIZE
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.math.BigDecimal
import java.sql.Date
import java.sql.ResultSet
import kotlin.test.assertEquals
import kotlin.test.assertTrue

abstract class BaseTestFixture {

  lateinit var closeFn: Close
  lateinit var resultSet: ResultSet

  @After fun closeDb() {
    closeFn.invoke()
  }

  open fun openDb() {
    with(H2MemoryDatabaseData.getCloseableResultSet()) {
      resultSet = first
      closeFn = second
    }
  }

}

class MetaDataTest : BaseTestFixture() {

  lateinit var metaData: ch.empros.kmap.MetaData

  @Before override fun openDb() {
    super.openDb()
    metaData = resultSet.kMapMetaData()
  }

  @Test fun `map`() {
    val expected = IntRange(0, metaData.colCount - 1).map { metaData[it] }
    val actual = metaData.map { it }
    assertEquals(expected, actual)
  }

  @Test fun `Getting KmColumn byName`() {
    val expected = metaData.map { it.name }.toSet()
    val actual = metaData.map { metaData.byName(it.name).name }.toSet()
    assertEquals(expected, actual)
  }

  @Test fun `IntColumn id`() {
    assertTrue(metaData[ID] is IntColumn)
    assertEquals(Int::class, (metaData[ID] as IntColumn).getVmType())
  }

  @Test fun `StringColumn firstname`() {
    assertTrue(metaData[FIRSTNAME] is StringColumn)
    assertEquals(String::class, (metaData[FIRSTNAME] as StringColumn).getVmType())
  }

  @Test fun `StringColumn lastname`() {
    assertTrue(metaData[LASTNAME] is StringColumn)
    assertEquals(String::class, (metaData[LASTNAME] as StringColumn).getVmType())
  }

  @Test fun `BooleanColumn active`() {
    assertTrue(metaData[ACTIVE] is BooleanColumn)
    assertEquals(Boolean::class, (metaData[ACTIVE] as BooleanColumn).getVmType())
  }

  @Test fun `DateColumn birth_date`() {
    assertTrue(metaData[BIRTH_DATE] is DateColumn)
    assertEquals(Date::class, (metaData[BIRTH_DATE] as DateColumn).getVmType())
  }

  @Test fun `DoubleColumn double_discount`() {
    assertTrue(metaData[DOUBLE_DISCOUNT] is DoubleColumn)
    assertEquals(Double::class, (metaData[DOUBLE_DISCOUNT] as DoubleColumn).getVmType())
  }

  @Test fun `FloatColumn float_discount`() {
    assertTrue(metaData[FLOAT_DISCOUNT] is FloatColumn)
    assertEquals(Float::class, (metaData[FLOAT_DISCOUNT] as FloatColumn).getVmType())
  }

  @Test fun `LongColumn size`() {
    assertTrue(metaData[SIZE] is LongColumn, "Expected LongColumn but got: ${metaData[SIZE]::class}")
    assertEquals(Long::class, (metaData[SIZE] as LongColumn).getVmType())
  }

  @Test fun `DecimalColumn valuation`() {
    assertTrue(metaData[VALUATION] is BigDecimalColumn, "Expected BigDecimalColumn but got: ${metaData[VALUATION]::class}")
    assertEquals(BigDecimal::class, (metaData[VALUATION] as BigDecimalColumn).getVmType())
  }
}

class PageFromResultSetTest : BaseTestFixture() {

  lateinit var page: Page

  @Before override fun openDb() {
    super.openDb()
    page = resultSet.toPageThenClose()
  }

  @Test fun `Records contain expected data`() {
    val expected = IntRange(1, 5).map {
      val birthDate = toSqlDate(VAL_BIRTH_DATE)
      mapOf(ID to it, FIRSTNAME to "$VAL_FIRSTNAME $it", LASTNAME to "$VAL_LASTNAME $it",
            ACTIVE to true, BIRTH_DATE to birthDate.toString(),
            DOUBLE_DISCOUNT to 1.5, FLOAT_DISCOUNT to .5f, SIZE to VAL_SIZE)
    }.toList()

    val actual = page.records.map {
      mapOf<String, Any>(ID to it[ID], FIRSTNAME to it[FIRSTNAME], LASTNAME to it[LASTNAME],
                         ACTIVE to it[ACTIVE], BIRTH_DATE to it.value(BIRTH_DATE).toString(),
                         DOUBLE_DISCOUNT to it[DOUBLE_DISCOUNT], FLOAT_DISCOUNT to it[FLOAT_DISCOUNT], SIZE to it[SIZE])
    }.toList()

    assertEquals(expected, actual)
  }
}