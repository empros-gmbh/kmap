package ch.empros.kmap

import io.reactivex.Observable
import io.reactivex.rxkotlin.toObservable

data class Record(val metaData: MetaData, private val data: List<Any?>) {
  fun value(label: String): Any? = data[metaData.indexOf(label)]

  inline operator fun <reified T> get(label: String): T = value(label) as T
}

/**
 * A [Page] object represents a query result.
 *
 */
class Page(val metaData: MetaData, val records: List<Record>) {

  val size: Int = records.size

  fun isEmpty(): Boolean = records.isEmpty()
  fun recordObs(): Observable<Record> = records.toObservable()

}
