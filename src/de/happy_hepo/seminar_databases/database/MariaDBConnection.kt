package de.happy_hepo.seminar_databases.database

import org.mariadb.jdbc.Connection
import java.sql.DriverManager

class MariaDBConnection(settings: DatabaseSettings) : IDatabaseConnection {
    private val connection = DriverManager.getConnection(
        "jdbc:mariadb://${settings.Host}${settings.Port?.let { ":$it" } ?: ""}/${settings.DatabaseName}",
        settings.User,
        settings.Password
    ) as Connection

    /**
     * Retrieves a simplified table structure from the database.
     *
     * DO NOT COPY THIS WITHOUT INPUT VALIDATION!
     * Input validation in this application is achieved through SQL parsing in the main part.
     *
     * @param table The table name to get the structure for.
     */
    override fun getTableStructure(table: String): List<Column> {
        val out = ArrayList<Column>()
        this.connection.createStatement().use { stmt ->
            val results = stmt.executeQuery("DESCRIBE $table;")
            while (results.next()) {
                out.add(
                    Column(
                        Name = results.getString("Field"),
                        Type = results.getString("Type"),
                        Key = results.getString("Key") == "PRI",
                    )
                )
            }
        }
        return out
    }

    override fun close() {
        this.connection.close()
    }
}