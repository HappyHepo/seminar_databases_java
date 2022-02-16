package de.happy_hepo.seminar_databases.database

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import java.io.Closeable
import java.io.File
import java.util.*

data class Column(
    val Name: String,
    val Type: String, // TODO Normalize Types
    val Key: Boolean,
)

@Serializable
data class DatabaseSettings(
    val Type: String,
    val Host: String,
    val Port: Int? = null,
    val User: String,
    val Password: String,
    val DatabaseName: String,
)

interface IDatabaseConnection : Closeable {
    fun getTableStructure(table: String): List<Column>
}

fun readConfigFile(filename: String): DatabaseSettings? {
    return File(filename).let {
        if (it.exists()) {
            Json.decodeFromString(it.readText(Charsets.UTF_8))
        } else {
            null
        }
    }
}

fun DatabaseConnectionFactory(filename: String): IDatabaseConnection? = readConfigFile(filename)?.let { DatabaseConnectionFactory(it) }

fun DatabaseConnectionFactory(settings: DatabaseSettings): IDatabaseConnection? =
    when (settings.Type.lowercase(Locale.getDefault())) {
        "mariadb" -> MariaDBConnection(settings)
        else -> null
    }
