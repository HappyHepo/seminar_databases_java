package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.Column

class DocumentStructure(
    val baseTable: QueryTable,
    val databaseColumns: List<Column>? = null,
) {
    val keys = this.databaseColumns?.filter { it.Key }?.map { it.Name } ?: emptyList()
    val name = this.baseTable.name
    val nested: MutableList<DocumentStructure> = ArrayList()
    val split: MutableList<String> = ArrayList()
    private val references
        get():List<String> {
            return (this.baseTable.relations[this.name]?.let { listOf(this.name) } ?: emptyList()) + split
        }

    override fun toString(): String {
        return this.toString(0).replace("\n", "")
    }

    fun toString(indentBy: Int, original: Int = indentBy): String {
        val indent = " ".repeat(indentBy)
        fun indent(by: Int) = " ".repeat(indentBy + by * original)
        fun formatNonBlank(data: String) =
            if (data.isEmpty()) {
                data
            } else {
                "\n$data\n$indent"
            }
        return "${" ".repeat(indentBy - original)}{\n" +
                "$indent\"Name\":\"${this.name}\",\n" +
                "$indent\"Key\":\"${this.keys.joinToString(",")}\",\n" +
                "$indent\"Columns\":{" +
                (this.databaseColumns?.joinToString(",\n") { "${indent(1)}\"${it.Name}\":\"${it.Type}\"" }
                    ?.let { formatNonBlank(it) }
                    ?: "") +
                "},\n" +
                "$indent\"Relations\":{" +
                this.baseTable.relations.entries
                    .joinToString(",\n") { (target, columns) ->
                        "${indent(1)}\"$target\":[\n" +
                                columns.joinToString(",\n") { (from, to) ->
                                    "${indent(2)}{\n" +
                                            "${indent(3)}\"from\":\"$from\",\n" +
                                            "${indent(3)}\"to\":\"$to\"\n" +
                                            "${indent(2)}}"
                                } +
                                "\n${indent(1)}]"
                    }.let { formatNonBlank(it) } +
                "},\n" +
                "$indent\"Referenced\":[" +
                this.references.joinToString(",\n") { "${indent(1)}\"$it\"" }
                    .let { formatNonBlank(it) } +
                "],\n" +
                "$indent\"Nested\":[" +
                this.nested.joinToString(",\n") { it.toString(indentBy + 2 * original, original) }
                    .let { formatNonBlank(it) } +
                "]\n" +
                "${" ".repeat(indentBy - original)}}"
    }
}