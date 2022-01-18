package de.happy_hepo.seminar_databases

/**
 * POJO to simplify the structure of a query for later
 */
class QueryTable(
    var Name: String,
    val Properties: MutableList<String> = ArrayList(),
    /**
     * Map of relations. Each Entry lists  the property mapping (this table > other table) for a given other table.
     * Obtained by traversing the JOINs of a query
     */
    val Relations: MutableMap<String, MutableList<Pair<String, String>>> = HashMap(),
) {
    fun merge(other: QueryTable) {
        if (other.Name != this.Name) {
            throw Exception("Tables do not match!") // TODO proper Exception
        }
        // Merge Props
        other.Properties.forEach {
            if (it !in this.Properties) {
                this.Properties.add(it)
            }
        }
        // Merge Relations
        other.Relations.forEach { (name, relations) ->
            this.Relations[name].let { existing ->
                if (existing == null) {
                    this.Relations[name] = relations
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
        return "$Name: {${Properties.joinToString(",")}}"
    }
}
