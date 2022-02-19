package de.happy_hepo.seminar_databases

/**
 * POJO to simplify the structure of a query for later
 */
class QueryTable(
    var name: String,
    val properties: MutableList<String> = ArrayList(),
    /**
     * Map of relations. Each Entry lists  the property mapping (this table > other table) for a given other table.
     * Obtained by traversing the JOINs of a query
     */
    val relations: MutableMap<String, MutableList<Pair<String, String>>> = HashMap(),
) {
    fun merge(other: QueryTable) {
        if (other.name != this.name) {
            throw Exception("Tables do not match!") // TODO proper Exception
        }
        // Merge Props
        other.properties.forEach {
            if (it !in this.properties) {
                this.properties.add(it)
            }
        }
        // Merge Relations
        other.relations.forEach { (name, relations) ->
            this.relations[name].let { existing ->
                if (existing == null) {
                    this.relations[name] = relations
                } else {
                    relations.forEach {
                        if (existing.find { (from, to) -> it.first == from && it.second == to } == null) {
                            existing.add(it)
                        }
                    }
                }
            }
        }
    }

    override fun toString(): String {
        return "$name: {${properties.joinToString(",")}}"
    }
}
