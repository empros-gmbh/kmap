package ch.empros.kmap

import org.junit.Test
import kotlin.test.assertFailsWith

class ResultSetExtSimpleTest {

  @Test fun `Calling dataSourceFrom with closed resultSet throws`() {
    val (rs, close) = H2MemoryDatabaseData.getCloseableResultSet()
    close()
    assertFailsWith(IllegalStateException::class,
                    "Closed ResultSet should cause exception"
    ) { rs.toPageThenClose() }
  }

}