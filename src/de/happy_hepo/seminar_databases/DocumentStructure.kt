package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.Column

class DocumentStructure(
    val baseTable: QueryTable,
    val databaseColumns: List<Column>? = null,
) {
    val keys = this.databaseColumns?.filter { it.Key }?.map { it.Name } ?: emptyList()
    val name = this.baseTable.name
    val nested: MutableList<DocumentStructure> = ArrayList()

    override fun toString(): String {
        return this.toString(0).replace("\n", "")
    }

    fun toString(indentBy: Int, original: Int = indentBy): String { // TODO
        val indent = " ".repeat(indentBy)
        return "${" ".repeat(indentBy - original)}{\n" +
                "$indent\"Name\":\"${this.name}\",\n" +
                "$indent\"Key\":\"${this.keys.joinToString(",")}\",\n" +
                "$indent\"Columns\":{" +
                (this.databaseColumns?.joinToString(",\n") { "$indent${" ".repeat(original)}\"${it.Name}\":\"${it.Type}\"" }
                    ?.let { "\n$it\n" }
                    ?: "") +
                "$indent},\n" +
                "$indent\"Nested\":[" +
                this.nested.joinToString(",\n") { it.toString(indentBy + 2 * original, original) }.let {
                    if (it.isEmpty()) {
                        it
                    } else {
                        "\n$it\n$indent"
                    }
                } +
                "]\n" +
                "${" ".repeat(indentBy - original)}}"
    }
}