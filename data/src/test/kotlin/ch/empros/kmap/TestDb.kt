package ch.empros.kmap

import java.math.BigDecimal
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.*

typealias Close = () -> Unit

object H2MemoryDatabaseData {

  private val DB_DRIVER = "org.h2.Driver"
  private val DB_CONNECTION = "jdbc:h2:mem:test"
  private val DB_USER = ""
  private val DB_PASSWORD = ""

  val SQL_SELECT_PERSON = "select * from PERSON"

  /**
   * These field names correspond to the Data in the in-memory db.
   */
  val ID = "id".toUpperCase()
  val FIRSTNAME = "firstName".toUpperCase()
  val LASTNAME = "lastName".toUpperCase()
  val ACTIVE = "active".toUpperCase()
  val BIRTH_DATE = "birthdate".toUpperCase()
  val FLOAT_DISCOUNT = "float_discount".toUpperCase()
  val DOUBLE_DISCOUNT = "double_discount".toUpperCase()
  val SIZE = "size".toUpperCase()
  val VALUATION = "vauation".toUpperCase()
  val DOC ="doc".toUpperCase()

  val VAL_BIRTH_DATE = "1966-01-17"
  val VAL_FIRSTNAME = "Vorname"
  val VAL_LASTNAME = "Nachname"
  val VAL_ACTIVE = true
  val VAL_DOUBLE_DISCOUNT = 1.5
  val VAL_FLOAT_DISCOUNT = .5f
  val VAL_SIZE = 10L
  val VAL_VALUATION = BigDecimal(2.5)
  val VAL_DOC = "Clob-Inhalt, z.B. XML, HTML, etc."

  @Throws(Exception::class)
  @JvmStatic fun main(args: Array<String>) {
    try {
      val (rs, close) = ch.empros.kmap.H2MemoryDatabaseData.getCloseableResultSet()

      while (rs.next()) {
        println("Id '${rs.getInt("id")}' Name: '${rs.getString("firstname")} ${rs.getString("lastname")}' active = '${rs.getString("active")}' Birthdate: '${rs.getDate("birthdate")}' Float Discount = '${rs.getFloat("float_discount")}' Double Discount = '${rs.getDouble("double_discount")}' Size: '${rs.getLong("size")}'")
      }
      close()
    } catch (e: java.sql.SQLException) {
      e.printStackTrace()
    }
  }

  @Throws(java.sql.SQLException::class)
  fun setupTestDb(size: Int): java.sql.Connection {
    val connection = ch.empros.kmap.H2MemoryDatabaseData.dbConnection
    ch.empros.kmap.H2MemoryDatabaseData.runSetupQueries(connection, size)
    return connection
  }

  @Throws(java.sql.SQLException::class)
  fun getCloseableResultSet(size: Int = 5): Pair<java.sql.ResultSet, ch.empros.kmap.Close> {
    val connection = ch.empros.kmap.H2MemoryDatabaseData.dbConnection
    ch.empros.kmap.H2MemoryDatabaseData.runSetupQueries(connection, size)

    val selectPreparedStatement = connection.prepareStatement(ch.empros.kmap.H2MemoryDatabaseData.SQL_SELECT_PERSON)
    val resultSet = selectPreparedStatement.executeQuery()

    return Pair(resultSet, {
      selectPreparedStatement.close()
      connection.close()
    })
  }

  private fun runSetupQueries(connection: java.sql.Connection, size: Int) {

    val CreateQuery = "CREATE TABLE PERSON($ID int primary key, $FIRSTNAME varchar(100), $LASTNAME varchar(100), $ACTIVE boolean, $BIRTH_DATE date, $FLOAT_DISCOUNT real, $DOUBLE_DISCOUNT double, $SIZE bigint, $VALUATION decimal, $DOC clob)"
    val InsertQuery = "INSERT INTO PERSON ($ID, $FIRSTNAME, $LASTNAME, $ACTIVE, $BIRTH_DATE, $FLOAT_DISCOUNT, $DOUBLE_DISCOUNT, $SIZE, $VALUATION, $DOC) values (?,?,?,?,?,?,?,?,?,?)"

    connection.autoCommit = false

    val createPreparedStatement = connection.prepareStatement(CreateQuery)
    createPreparedStatement!!.executeUpdate()
    createPreparedStatement.close()

    with(connection.prepareStatement(InsertQuery)) {
      for (i in 1..size) {
        setInt(1, i)
        setString(2, "$VAL_FIRSTNAME $i")
        setString(3, "$VAL_LASTNAME $i")
        setBoolean(4, VAL_ACTIVE)
        setDate(5, toSqlDate(VAL_BIRTH_DATE))
        setFloat(6, VAL_FLOAT_DISCOUNT)
        setDouble(7, VAL_DOUBLE_DISCOUNT)
        setLong(8, VAL_SIZE.toLong())
        setBigDecimal(9, VAL_VALUATION)
        setClob(10, VAL_DOC.reader())
        executeUpdate()
      }
      close()
    }
    connection.commit()

  }

  private val dbConnection: java.sql.Connection
    get() {
      try {
        Class.forName(ch.empros.kmap.H2MemoryDatabaseData.DB_DRIVER)
      } catch (e: ClassNotFoundException) {
        println(e.message)
      }
      return java.sql.DriverManager.getConnection(ch.empros.kmap.H2MemoryDatabaseData.DB_CONNECTION, ch.empros.kmap.H2MemoryDatabaseData.DB_USER, ch.empros.kmap.H2MemoryDatabaseData.DB_PASSWORD)
    }
}

fun toSqlDate(dateString: String, format: String = "yyyy-MM-dd"): Date {
  val simpleDateFormat = SimpleDateFormat(format)
  simpleDateFormat.timeZone = TimeZone.getTimeZone("UTC")
  return Date(simpleDateFormat.parse(dateString).time)
}
