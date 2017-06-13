package ch.empros.kmap

interface Query {
  val pagedQuery: String
  val pageSize: Int
  val startPage: Int
  fun nextPage(): Query
}

/**
 * A [SqlQuery] object can be used to create pageable data sources, see [KMap.pageObservableFrom].
 * Note: [startPage] is zero-based, ie. the first page is at index 0.
 */
data class SqlQuery(private val query: String, override val pageSize: Int = 50, override val startPage: Int = 0) : Query {

  init {
    require(pageSize > 0, { "PageSize must be > 0, but got: $pageSize" })
    require(startPage > -1, { "StartPage must >= 0, but got: $startPage" })
  }

  override val pagedQuery = "$query LIMIT $pageSize OFFSET ${startPage * pageSize}"

  override fun nextPage() = copy(startPage = startPage + 1)

}
