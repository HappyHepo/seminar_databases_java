package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.Column

class DocumentStructure(
    val BaseTable: QueryTable,
    val DatabaseColumns: List<Column>? = null,
) {
    var weight = 0
    val keys = this.DatabaseColumns?.filter { it.Key }?.map { it.Name } ?: emptyList()

    fun clone(): DocumentStructure {
        val out = DocumentStructure(this.BaseTable, this.DatabaseColumns)
        out.weight = this.weight
        return out
    }

   override fun toString(): String { // TODO
        return "TODO"
    }
}