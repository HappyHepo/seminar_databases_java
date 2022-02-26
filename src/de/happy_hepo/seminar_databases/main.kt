package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.DatabaseConnectionFactory
import de.happy_hepo.seminar_databases.ui.*
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.alg.cycle.CycleDetector
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import java.io.File

typealias QueryStructure = Map<String, QueryTable>

fun main(args: Array<String>) {
    if (args.isNotEmpty()) {
        parseSqlFile(args[0])?.let {
            // merge queries to Graph
            val graph = createGraph(it)

            val documents = createSchemaDesign(graph,"config.json", getUI())

            File(args.getOrElse(1) { "result.json" }).let { file ->
                if (!file.exists()) {
                    file.createNewFile()
                } else {
                    // TODO ask if overwrite
                }
                file.writeText("[${documents.joinToString(",\n") { doc -> doc.toString(4) }}]")
            }
            return
        }
    }
    println("No file for parsing found!")
}

fun createGraph(queries: List<QueryStructure>): DefaultDirectedGraph<QueryTable, DefaultEdge> {
    val dg = DefaultDirectedGraph<QueryTable, DefaultEdge>(DefaultEdge::class.java)
    val relationsPile = HashSet<Pair<String, String>>()
    fun addToPile(tables: Pair<String, String>) {
        relationsPile.find { (first, second) -> first == tables.first && second == tables.second }
            ?: run { relationsPile.add(tables) }
    }

    fun addRelation(tables: Pair<String, String>) {
        dg.vertexSet().find { it.name == tables.second }.let { target ->
            if (target != null) {
                dg.addEdge(dg.vertexSet().find { it.name == tables.first }, target)
            } else {
                addToPile(tables)
            }
        }
    }

    fun addTableRelations(table: QueryTable) {
        table.relations.keys.forEach { other ->
            addRelation(Pair(table.name, other))
        }
    }

    queries.forEach { query ->
        query.forEach { (name, table) ->
            dg.vertexSet().find { it.name == name }.let { vertex ->
                if (vertex != null) {
                    vertex.merge(table)
                    addTableRelations(vertex)
                } else {
                    dg.addVertex(table)
                    addTableRelations(table)
                    relationsPile.filter { (_, target) -> target == name }.forEach(::addRelation)
                    relationsPile.removeIf { (_, target) -> target == name }
                }
            }
        }
    }
    return dg
}

// TODO Check self referencing queries for parent/child attributes, add to document
// TODO cyclic graphs currently not supported
//  idea: identify the cycle and ask the user to pick a split
fun createSchemaDesign(
    graph: DefaultDirectedGraph<QueryTable, DefaultEdge>,
    configFile: String,
    ui: IUi,
): List<DocumentStructure> {
    DatabaseConnectionFactory(configFile)?.use { connection ->
        val finalDocuments = ConnectivityInspector(graph).connectedSets()
            .filter { tables ->
                val dg = DefaultDirectedGraph<QueryTable, DefaultEdge>(DefaultEdge::class.java)
                tables.forEach(dg::addVertex)
                tables.forEach { vertex ->
                    vertex.relations.keys
                        .filter { otherTable -> otherTable != vertex.name }
                        .forEach { otherTable -> dg.addEdge(vertex, tables.find { it.name == otherTable }) }
                }
                if (CycleDetector(dg).detectCycles()) {
                    // TODO Identify cycle
                    //  if no edges point into cycle from other part of graph
                    //   then what??
                    //   else start from the vertexes that have no predecessors
                    println("Queries contain cycles! Not supported")
                    false
                } else {
                    true
                }
            }
            .map { tables -> tables.map { DocumentStructure(it, connection.getTableStructure(it.name)) } }
            .map { documents ->
                documents.forEach { document ->
                    document.baseTable.relations
                        .filter { (_, columns) -> document.keys.containsAll(columns.map { it.first }) }
                        .forEach { (otherTable, columns) ->
                            documents
                                .find { it.name == otherTable && !(it.keys.containsAll(columns.map { col -> col.second }) && it.keys.size >= document.keys.size) }
                                ?.let { switchRelation(document, it) }
                        }
                }

                val subGraph = DefaultDirectedGraph<DocumentStructure, DefaultEdge>(DefaultEdge::class.java)
                documents.forEach(subGraph::addVertex)
                documents.forEach { vertex ->
                    vertex.baseTable.relations.keys.forEach { otherTable -> subGraph.addEdge(vertex, documents.find { it.name == otherTable }) }
                }

                subGraph
            }
            .flatMap { subGraph ->
                val tables = subGraph.vertexSet().associateBy { it.name }
                val documentCache = HashMap<String, DocumentStructure>()
                fun getOrigins(tables: Collection<DocumentStructure>): List<String> {
                    val successors = tables.flatMap { table -> table.baseTable.relations.filter { (otherTable, _) -> otherTable != table.name }.keys }
                    return tables.filter { table -> !successors.contains(table.name) }.map { it.name }
                }

                fun generateFullExtension(table: String): DocumentStructure {
                    val current = documentCache[table]?.let { return it } ?: tables[table]!!
                    current.nested.addAll(current.baseTable.relations.keys.map(::generateFullExtension))
                    documentCache[table] = current
                    return current
                }

                var origins = getOrigins(tables.values)
                val additionalDocuments = mutableSetOf<String>()
                val approved = ArrayList<String>()
                do {
                    val root = origins.find { !approved.contains(it) } ?: additionalDocuments.find { !approved.contains(it) }!!
                    val extended = generateFullExtension(root)
                    when (val result = ui.askApproval(extended)) {
                        ApprovalAnswer.Approve -> approved.add(root)
                        ApprovalAnswer.Reverse -> {
                            result.data
                                ?.mapNotNull { tables[it] }
                                ?.let { reverseList ->
                                    reverseList
                                        .mapIndexedNotNull { i, current ->
                                            if (i == 0) {
                                                null
                                            } else {
                                                Pair(reverseList[i - 1], current)
                                            }
                                        }
                                        .forEach {
                                            switchRelation(it.first, it.second)
                                        }
                                }
                        }
                        ApprovalAnswer.Duplicate, ApprovalAnswer.Split -> {
                            result.data?.let { (parent, child) ->
                                additionalDocuments.add(child)
                                if (result == ApprovalAnswer.Split) {
                                    tables[parent]?.nested?.removeIf { it.name == child }
                                    tables[parent]?.split?.add(child)
                                }
                            }
                        }
                    }
                    origins = getOrigins(tables.values)
                } while (!(approved.containsAll(origins) && approved.containsAll(additionalDocuments)))

                approved.map { tables[it] }
            }.filterNotNull()
        // TODO Kardinalitäten bestimmen
        // TODO Redundanzen/Beziehungen der Objekte ermitteln
        return finalDocuments
    }
    return emptyList()
}

fun switchRelation(first: DocumentStructure, second: DocumentStructure) {
    second.baseTable.relations[first.name] =
        (first.baseTable.relations[second.name]?.map { it.second to it.first } ?: emptyList()).toMutableList()
    first.baseTable.relations.remove(second.name)
    first.nested.find { it.name == second.name }?.let {
        first.nested.remove(it)
        second.nested.add(first)
    }
}