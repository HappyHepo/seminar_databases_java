package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.database.DatabaseConnectionFactory
import de.happy_hepo.seminar_databases.database.IDatabaseConnection
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
    // TODO Zusätzliche Infos aus DB ziehen? Spalten, welche noch?
    DatabaseConnectionFactory(configFile)?.use { connection ->
        val connectedGraphs = ConnectivityInspector(graph).connectedSets().map { vertices ->
            val dg = DefaultDirectedGraph<QueryTable, DefaultEdge>(DefaultEdge::class.java)
            vertices.forEach(dg::addVertex)
            vertices.forEach { vertex ->
                vertex.Relations.keys
                    .filter { otherTable -> otherTable != vertex.Name }
                    .forEach { otherTable -> dg.addEdge(vertex, vertices.find { it.Name == otherTable }) }
            }
            if (CycleDetector(dg).detectCycles()) {
                // TODO Identify cycle
                //  if no edges point into cycle from other part of graph
                //   then what??
                //   else start from the vertexes that have no predecessors
                println("Queries contain cycles! Not supported")
                null
            } else {
                dg
            }
        }.filterNotNull()
            .forEach { subGraph ->
                val origins = subGraph.vertexSet().let { tables ->
                    val successors = tables.flatMap { table -> table.Relations.filter{(otherTable,_)->otherTable!=table.Name}.keys }
                    tables.filter { table -> !successors.contains(table.Name) }
                }
                val userTables = subGraph.vertexSet()
                userTables.forEach { table ->
                    val documents = ArrayList<DocumentStructure>()
                    val structure = DocumentStructure(table, connection.getTableStructure(table.Name))
                    documents.add(structure)
                    table.Relations.forEach { (otherTable, columns) ->
                        /*queries.filter { query ->
                            (query[table.Name]?.Properties?.containsAll(table.Properties) ?: false) &&
                                    (query[otherTable]?.Properties?.containsAll(columns.map { it.second }) ?: false)
                        }.forEach {
                            val tableWithQuery = structure.clone()
                            tableWithQuery.weight += it.size
                        }*/
                    }
                }
            }
    }
}

fun generateExtensions(table: QueryTable, connection: IDatabaseConnection): List<DocumentStructure> {
    val documents = ArrayList<DocumentStructure>()
    val structure = DocumentStructure(table, connection.getTableStructure(table.Name))
    documents.add(structure)
    return documents
}