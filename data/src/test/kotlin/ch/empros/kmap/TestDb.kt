package ch.empros.kmap

import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.*


typealias Close = () -> Unit

object H2MemoryDatabaseData {

    private const val DB_DRIVER = "org.h2.Driver"
    private const val DB_CONNECTION = "jdbc:h2:mem:test"
    private const val DB_USER = ""
    private const val DB_PASSWORD = ""

    const val SQL_SELECT_PERSON = "select * from PERSON"

    /**
     * These field names correspond to the Data in the in-memory db.
     */
    val ID = "id".uppercase(Locale.getDefault())
    val FIRSTNAME = "firstName".uppercase(Locale.getDefault())
    val LASTNAME = "lastName".uppercase(Locale.getDefault())
    val ACTIVE = "active".uppercase(Locale.getDefault())
    val BIRTH_DATE = "birthdate".uppercase(Locale.getDefault())
    val FLOAT_DISCOUNT = "float_discount".uppercase(Locale.getDefault())
    val DOUBLE_DISCOUNT = "double_discount".uppercase(Locale.getDefault())
    val SIZE = "size".uppercase(Locale.getDefault())
    val VALUATION = "valuation".uppercase(Locale.getDefault())
    val DOC = "doc".uppercase(Locale.getDefault())
    val TIMESTAMP = "time_stamp".uppercase(Locale.getDefault())

    const val VAL_BIRTH_DATE = "1966-01-17"
    const val VAL_FIRSTNAME = "Vorname"
    const val VAL_LASTNAME = "Nachname"
    private const val VAL_ACTIVE = true
    private const val VAL_DOUBLE_DISCOUNT = 1.5
    private const val VAL_FLOAT_DISCOUNT = .5f
    const val VAL_SIZE = 10L
    private val VAL_VALUATION = BigDecimal(2.5)
    private const val VAL_DOC = "Clob-Inhalt, z.B. XML, HTML, etc."
    val VAL_TIMESTAMP: Timestamp = Calendar.getInstance().let { calendar ->
        calendar.set(2018, 2, 10, 10, 11, 30)
        Timestamp(calendar.timeInMillis)
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val (rs, close) = getCloseableResultSet()

            while (rs.next()) {
                println(
                    "Id '${rs.getInt("id")}' Name: '${rs.getString("firstname")} ${rs.getString("lastname")}' active = '${
                        rs.getString(
                            "active"
                        )
                    }' Birthdate: '${rs.getDate("birthdate")}' Float Discount = '${rs.getFloat("float_discount")}' Double Discount = '${
                        rs.getDouble(
                            "double_discount"
                        )
                    }' Size: '${rs.getLong("size")}'"
                )
            }
            close()
        } catch (e: java.sql.SQLException) {
            e.printStackTrace()
        }
    }

    @Throws(java.sql.SQLException::class)
    fun setupTestDb(size: Int): java.sql.Connection {
        val connection = dbConnection
        runSetupQueries(connection, size)
        return connection
    }

    @Throws(java.sql.SQLException::class)
    fun getCloseableResultSet(size: Int = 5): Pair<java.sql.ResultSet, Close> {
        val connection = dbConnection
        runSetupQueries(connection, size)

        val selectPreparedStatement = connection.prepareStatement(SQL_SELECT_PERSON)
        val resultSet = selectPreparedStatement.executeQuery()

        return Pair(resultSet) {
            selectPreparedStatement.close()
            connection.close()
        }
    }

    private fun runSetupQueries(connection: java.sql.Connection, size: Int) {

        val createQuery =
            "CREATE TABLE PERSON($ID int primary key, $FIRSTNAME varchar(100), $LASTNAME varchar(100), $ACTIVE boolean, $BIRTH_DATE date, $FLOAT_DISCOUNT real, $DOUBLE_DISCOUNT double, $SIZE bigint, $VALUATION decimal, $DOC clob, $TIMESTAMP timestamp)"
        val insertQuery =
            "INSERT INTO PERSON ($ID, $FIRSTNAME, $LASTNAME, $ACTIVE, $BIRTH_DATE, $FLOAT_DISCOUNT, $DOUBLE_DISCOUNT, $SIZE, $VALUATION, $DOC, $TIMESTAMP) values (?,?,?,?,?,?,?,?,?,?,?)"

        connection.autoCommit = false

        val createPreparedStatement = connection.prepareStatement(createQuery)
        createPreparedStatement!!.executeUpdate()
        createPreparedStatement.close()

        with(connection.prepareStatement(insertQuery)) {
            for (i in 1..size) {
                setInt(1, i)
                setString(2, "$VAL_FIRSTNAME $i")
                setString(3, "$VAL_LASTNAME $i")
                setBoolean(4, VAL_ACTIVE)
                setDate(5, toSqlDate(VAL_BIRTH_DATE))
                setFloat(6, VAL_FLOAT_DISCOUNT)
                setDouble(7, VAL_DOUBLE_DISCOUNT)
                setLong(8, VAL_SIZE)
                setBigDecimal(9, VAL_VALUATION)
                setClob(10, VAL_DOC.reader())
                setTimestamp(11, VAL_TIMESTAMP)
                executeUpdate()
            }
            close()
        }
        connection.commit()

    }

    private val dbConnection: java.sql.Connection
        get() {
            try {
                Class.forName(DB_DRIVER)
            } catch (e: ClassNotFoundException) {
                println(e.message)
            }
            return java.sql.DriverManager.getConnection(
                DB_CONNECTION,
                DB_USER,
                DB_PASSWORD
            )
        }
}

fun toSqlDate(dateString: String, format: String = "yyyy-MM-dd"): Date {
  val simpleDateFormat = SimpleDateFormat(format)
  simpleDateFormat.timeZone = TimeZone.getTimeZone("UTC")
  return Date(simpleDateFormat.parse(dateString).time)
}
