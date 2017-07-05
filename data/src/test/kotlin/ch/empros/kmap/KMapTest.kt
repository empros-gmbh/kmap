package ch.empros.kmap

import ch.empros.kmap.H2MemoryDatabaseData.ID
import ch.empros.kmap.H2MemoryDatabaseData.SQL_SELECT_PERSON
import org.junit.After
import org.junit.Test
import java.sql.Connection
import kotlin.test.assertEquals

class KMapTest {

  @Test fun `Take all`() {
    val size = 9
    setupTestDb(size)
    val query = SqlQuery(SQL_SELECT_PERSON, pageSize = 4)

    val expectedPageCount = size / query.pageSize + 1

    KMap.pageObservableFrom(supplier, query)
      .take(size.toLong())
      .test()
      .assertValueCount(expectedPageCount)
      .assertComplete()
  }

  @Test fun `Take second page only`() {
    val size = 9
    val pageSize = 4
    setupTestDb(size)

    val expected = IntRange(5, 8).map { it }.toList()

    KMap.pageObservableFrom(supplier, SqlQuery(SQL_SELECT_PERSON, pageSize = pageSize, startPage = 1))
      .take(1)
      .flatMap { page -> page.recordObs() }
      .map { rec -> rec.value(ID)!! }
      .test()
      .assertValueSequence(expected)
      .assertComplete()
  }

  @Test fun `Trying to take an unavailable page completes with an empty page`() {
    val size = 9
    val pageSize = 4
    setupTestDb(size)

    KMap.pageObservableFrom(supplier, SqlQuery(SQL_SELECT_PERSON, pageSize = pageSize, startPage = 5))
      .take(pageSize.toLong())
      .filter { it.isEmpty() }
      .test()
      .assertValueCount(1)
      .assertComplete()
  }

  @Test fun `Subscribe twice`() {
    val size = 9
    val pageSize = 4
    setupTestDb(size)

    val expectedPageCount = size / pageSize + 1

    val ds = KMap.pageObservableFrom(supplier, SqlQuery(SQL_SELECT_PERSON, pageSize = pageSize, startPage = 0))
    ds.test()
      .assertValueCount(expectedPageCount)
      .assertComplete()
    ds.test()
      .assertValueCount(expectedPageCount)
      .assertComplete()
  }

  @Test fun `toList and blockingGet`() {
    val size = 9
    val pageSize = 4
    setupTestDb(size)

    val expectedPageCount = size / pageSize + 1

    val ds = KMap.pageObservableFrom(supplier, SqlQuery(SQL_SELECT_PERSON, pageSize = pageSize, startPage = 0))
    val actual = ds.toList().blockingGet()
    assertEquals(expectedPageCount, actual.size, "List should contain all pages")
  }

  lateinit private var connection: Connection
  lateinit private var supplier: StatementSupplier

  private fun setupTestDb(size: Int) {
    connection = H2MemoryDatabaseData.setupTestDb(size)
    supplier = SingleConnectionSupplier(connection)
  }

  @After fun tearDown() {
    connection.close()
  }

}