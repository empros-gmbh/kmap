package ch.empros.kmap

import org.junit.Test

class QueryTest {
  @Test fun `Query with a pageSize lt 1 throws`() {
    kotlin.test.assertFailsWith(java.lang.IllegalArgumentException::class, "pageSize < 1:", { SqlQuery("", pageSize = 0) })
  }

  @Test fun `Paging with a pageIndex  lt 0 throws`() {
    kotlin.test.assertFailsWith(java.lang.IllegalArgumentException::class, "startPage < 0:", { SqlQuery("", startPage = -1) })
  }

  @Test fun `pagedQuery returns correct limit and offset string`() {
    kotlin.test.assertEquals(" limit 10 offset 10".toUpperCase(), SqlQuery("", pageSize = 10, startPage = 1).pagedQuery, "wrong limit string")
  }

  @Test fun `nextPage`() {
    kotlin.test.assertEquals(1, SqlQuery("", startPage = 0).nextPage().startPage, "Page should be 1")
  }
}