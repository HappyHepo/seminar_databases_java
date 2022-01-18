package de.happy_hepo.seminar_databases

import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge


typealias QueryStructure = Map<String, QueryTable>

fun main(args: Array<String>) {
    if (args.isNotEmpty()) {
        parseSqlFile(args[0])?.let {
            // TODO merge List to Graph
            val graph = createGraph(it)
            // createSchemaDesign(it)
            return
        }
    }
    println("No file for parsing found!")
}

fun createGraph(queries: List<QueryStructure>): DefaultDirectedGraph<QueryTable, DefaultEdge> {
    val dag = DefaultDirectedGraph<QueryTable, DefaultEdge>(DefaultEdge::class.java)
    val relationsPile = HashSet<Pair<String, String>>()
    fun addToPile(tables: Pair<String, String>) {
        relationsPile.find { (first, second) -> first == tables.first && second == tables.second }
            ?: run { relationsPile.add(tables) }
    }

    fun addRelation(tables: Pair<String, String>) {
        dag.vertexSet().find { it.Name == tables.second }.let { target ->
            if (target != null) {
                dag.addEdge(dag.vertexSet().find { it.Name == tables.first }, target)
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
            dag.vertexSet().find { it.Name == name }.let { vertex ->
                if (vertex != null) {
                    vertex.merge(table)
                    addTableRelations(vertex)
                } else {
                    dag.addVertex(table)
                    addTableRelations(table)
                    relationsPile.filter { (_, target) -> target == name }.forEach(::addRelation)
                    relationsPile.removeIf { (_, target) -> target == name }
                }
            }
        }
    }
    return dag
}

fun createSchemaDesign(graph: DefaultDirectedGraph<QueryTable, DefaultEdge>, queries: List<QueryStructure>) {
//    Input:
//    graph gives:
//    var userTables- A list of relational database tables with table structure,
//    var relationArrayList –A list of relationship of relational tables(Format given in Table 1),
//    queries gives:
//    var selectQueryJSON –A list select queries in JSON file (Format given in Figure 2(a)),
//    var updateQueryJSON –A list of update queries in JSON file (Format given in Figure 2(b)).
//
//    Output: All possible combination of each relational tables in JSON format, after applying select and update queries, in
//    tableList[][] for document store database of NoSQL.
    val userTables = graph.vertexSet()
//    Begin
//    for each table x in userTables, create t document as a JSON structure
    userTables.forEach { table ->
//    Begin
//    var STab[] //It is a one dimensional array on JSON object.
        val documents = ArrayList<DocumentStructure>()
//    //Now t JSON has key/value corresponding to columnName/dataType of x table.
//    Add updateWeight=0 and selectWeight= 0 to t as key/Value.
//    Add t in STab[].
        documents.add(DocumentStructure(table))
//    for each relation rs in relationArrayList
//    Begin
//    If t and keys of t is available in (p_table, p_column) of relationArrayList
//    Begin
//    for each(t.key and corresponding elements of list[][] are in select-list of each selectQueryJSON query )
//    Begin
//    1. Add/ update each select queries select-list with intermediate table’s primary key in t{} as embed with c_table as
//    keyName (c_table of Table 1).
//    2. Add/update Where clauses column-tablename in t{} as embed with c_table as keyName (Only Child Table Columns
//    will be added/updated in Parent table).
//    3. Update selectWeight = selectWeight+CurrentSelectQueryWeight in t{}.
//    4. if Adding select-list data to t{} with those column that are in update query of updateQueryJSON (update columns
//    must be of different tables then t{}) then Update updateWeight = updateWeight+CurrentUpdateQueryWeight in t{}.
//    5. For each updated t{}, Updated t{} can be renamed to tQueryID and add tQueryID in list STab[].
//    End for;
//    End if;
//    End for;
//    Add STab[] in tableList[][]
//    End for;
    }
//    End Algorithm 1.
}