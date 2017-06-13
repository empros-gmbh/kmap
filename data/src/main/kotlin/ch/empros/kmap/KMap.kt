package ch.empros.kmap

import io.reactivex.Observable
import io.reactivex.Observable.defer
import io.reactivex.Observable.empty
import java.sql.Connection
import java.sql.PreparedStatement

object KMap {

  /**
   * Creates an [Observable] of [Page] objects based on a [selectQuery].
   *
   * This method supports paging. The returned observable is lazy, the underlying SQL statement is only executed upon
   * subscription. Furthermore, the returned Observable is guaranteed to complete when there are no more records to
   * read, ie. the query is exhausted.
   *
   * It is best practice to constrain the number of records in a downstream observable by using operators like
   * [Observable.take]. Otherwise the upstream observable will continue to stream [Page]s as long as there is data.
   * This of course might be what you want exactly, if you need to process all data.
   *
   * The returned observable emits at least one [Page] object. If the query yields an empty resultSet,
   * this page is empty.
   */
  fun pageObservableFrom(statementSupplier: StatementSupplier,
                         selectQuery: Query): Observable<Page> {

    fun deferred(query: Query): Observable<Page> {

      var exhausted = false

      return defer {
        statementSupplier.createStatement(query).use {
          val page = it.executeQuery().toPageThenClose()
          exhausted = page.size < query.pageSize
          Observable.just(page)
        }
      }
        .concatWith(defer {
          if (exhausted) empty() else deferred(query.nextPage())
        })
    }

    return deferred(selectQuery)
  }

}

interface StatementSupplier {
  fun createStatement(query: Query): PreparedStatement
}

class SingleConnectionSupplier(private val connection: Connection) : StatementSupplier {
  override fun createStatement(query: Query): PreparedStatement {
    return connection.prepareStatement(query.pagedQuery)
  }
}

fun <T> PreparedStatement.use(block: (PreparedStatement) -> T): T {
  val result = block(this)
  close()
  return result
}

