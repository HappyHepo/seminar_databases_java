package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.DatabaseConnectionFactory
import de.happy_hepo.seminar_databases.ui.*
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.alg.cycle.CycleDetector
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge

typealias QueryStructure = Map<String, QueryTable>

fun main(args: Array<String>) {
    if (args.isNotEmpty()) {
        parseSqlFile(args[0])?.let {
            // merge List to Graph
            val graph = createGraph(it)
            // TODO Enrich the graph with table data (PK)
            createSchemaDesign(
                graph, /*it,*/ "config.json"
            )
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
        dg.vertexSet().find { it.Name == tables.second }.let { target ->
            if (target != null) {
                dg.addEdge(dg.vertexSet().find { it.Name == tables.first }, target)
            } else {
                addToPile(tables)
            }
        }
    }

    fun addTableRelations(table: QueryTable) {
        table.Relations.keys.forEach { other ->
            addRelation(Pair(table.Name, other))
        }
    }

    queries.forEach { query ->
        query.forEach { (name, table) ->
            dg.vertexSet().find { it.Name == name }.let { vertex ->
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

// TODO Überlegungen: Zyklen in Graphen identifizieren
//  non-Zyklische Graphen: hierarchisch auflösen
//  Graphen optimierter Algorithmus analog der Graphen paper

// TODO Graphen algorithmus zur bestimmung der verbindungen, Schema aus Tabelle zur Bestimmung der Properties/Keys, verschiedene Dokumenten Schemata generieren
// TODO Algorithmus: Primary Key/Foreign Table egal, geht nur um gerichtete Beziehung, einmal ohne Algorithmus probieren
// TODO Bei Hierarchischen Queries prüfen, welche Eltern/Kind-Attribute benötigt werden; in Dokument aufnehmen
// TODO Tabellen mit PK als input, um anhand von PK zu identifizieren, was Part ist und was Parent ist - MySQL - DESCRIBE table; Filter auf Key = 'PRI';
//  Optional Input mit Tabellen, deren Einträge einzelnes Objekt identifizieren? Brauche ich das? Könnte zuviel werden
// TODO Beziehungen in Queries müssen gerichtet sein, damit JOINS durchgeführt werden können, d.h. Reihenfolge matters
//  außer wenn die Beziehung ungerichtet ist, es kann aber nicht gerichtet in die falsche Richtung sein
//  => Doch
fun createSchemaDesign(
    graph: DefaultDirectedGraph<QueryTable, DefaultEdge>,
    // queries: List<QueryStructure>,
    configFile: String,
) {
    val ui: IUi = CliUi()
    // TODO Zusätzliche Infos aus DB ziehen? Spalten, welche noch?
    DatabaseConnectionFactory(configFile)?.use { connection ->
        val connectedGraphs = ConnectivityInspector(graph).connectedSets()
            .filter { tables ->
                val dg = DefaultDirectedGraph<QueryTable, DefaultEdge>(DefaultEdge::class.java)
                tables.forEach(dg::addVertex)
                tables.forEach { vertex ->
                    vertex.Relations.keys
                        .filter { otherTable -> otherTable != vertex.Name }
                        .forEach { otherTable -> dg.addEdge(vertex, tables.find { it.Name == otherTable }) }
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
            .map { tables -> tables.map { DocumentStructure(it, connection.getTableStructure(it.Name)) } }
            .map { documents ->
                documents.forEach { document ->
                    document.baseTable.Relations
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
                    vertex.baseTable.Relations.keys.forEach { otherTable -> subGraph.addEdge(vertex, documents.find { it.name == otherTable }) }
                }

                subGraph
            }
            .flatMap { subGraph ->
                val tables = subGraph.vertexSet().associateBy { it.name }
                val documentCache = HashMap<String, DocumentStructure>()
                fun getOrigins(tables: Collection<DocumentStructure>): List<String> {
                    val successors = tables.flatMap { table -> table.baseTable.Relations.filter { (otherTable, _) -> otherTable != table.name }.keys }
                    return tables.filter { table -> !successors.contains(table.name) }.map { it.name }
                }

                fun generateFullExtension(table: String): DocumentStructure {
                    val current = documentCache[table]?.let { return it } ?: tables[table]!!
                    current.nested.addAll(current.baseTable.Relations.keys.map(::generateFullExtension))
                    documentCache[table] = current
                    return current;
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
                                }
                            }
                        }
                    }
                    origins = getOrigins(tables.values)
                } while (!(approved.containsAll(origins) && approved.containsAll(additionalDocuments)))

                approved.map { tables[it] }
            }
        // TODO Kardinalitäten bestimmen
        // TODO Redundanzen/Beziehungen der Objekte ermitteln
    }
}

fun switchRelation(first: DocumentStructure, second: DocumentStructure) {
    second.baseTable.Relations[first.name] =
        (first.baseTable.Relations[second.name]?.map { it.second to it.first } ?: emptyList()).toMutableList()
    first.baseTable.Relations.remove(second.name)
    first.nested.find { it.name == second.name }?.let {
        first.nested.remove(it)
        second.nested.add(first)
    }
}