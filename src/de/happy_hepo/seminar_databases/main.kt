package de.happy_hepo.seminar_databases

import de.happy_hepo.seminar_databases.extensions.clone
import de.happy_hepo.seminar_databases.extensions.similar
import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.expression.operators.relational.InExpression
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.parser.SimpleNode
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.*
import java.io.File
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap


fun main(args: Array<String>) {
    if (args.isNotEmpty()) {
        parseSqlFile(args[0])?.run {
            // TODO merge List to Graph
            return
        }
    }
    println("No file for parsing found!")
}

fun parseSqlFile(filename: String): List<QueryStructure>? {
    val sql = File(filename).let {
        if (it.exists()) {
            it.readText(Charsets.UTF_8)
        } else {
            return null
        }
    }
    val queries = CCJSqlParserUtil.parseStatements(sql)

    val cleaned = queries.statements
        .partition { it is Select && it.selectBody is PlainSelect }
        .let { (select, notSelect) ->
            notSelect.forEach { unknown(it, "Not a Select") }
            @Suppress("UNCHECKED_CAST") // That's what the partition does...
            select as List<Select>
        }
        .flatMap { cleanUnionWith(it) }
        .flatMap { cleanUnionFrom(it) }
        .flatMap { cleanUnionJoin(it) }
        .flatMap { cleanUnionCondition(it) }
        .map { cleanWith(it) }
        .map { cleanSubSelect(it) }

    return cleaned.mapNotNull{ query ->
        query.selectBody?.let { body ->
            if (body is PlainSelect) {
                QueryStructure(parseSelect(body).associateBy { it.Name })
            } else {
                null
            }
        }
    }
}

/**
 * Splits a SQL query on UNION in FROM
 * Does not account for nested UNIONs in a single query
 */
fun cleanUnionFrom(query: Select): List<Select> {
    (query.selectBody as PlainSelect).fromItem.let { maybeUnion ->
        if (maybeUnion is SubSelect) {
            maybeUnion.selectBody.let { body ->
                if (body is SetOperationList) { // Union
                    val out = ArrayList<Select>()
                    (maybeUnion.selectBody as SetOperationList).selects.forEach { subSelect ->
                        val cleaned = query.clone()
                        (cleaned.selectBody as PlainSelect).let { select ->
                            select.fromItem = SubSelect().withSelectBody(subSelect).withAlias(select.fromItem.alias)
                            out.add(cleaned)
                        }
                    }
                    return out
                }
            }
        }
    }
    return arrayListOf(query)
}

/**
 * Splits a SQL query on UNIONs in CTEs
 */
fun cleanUnionWith(query: Select): List<Select> {
    if (query.withItemsList?.isNotEmpty() == true) {
        var out = ArrayList<ArrayList<WithItem>>().also { it.add(ArrayList()) }
        val withSelectMap = HashMap<String, ArrayList<PlainSelect>>()
        val withMap = query.withItemsList.associateBy { it.name }
        val cteNames = withMap.keys

        query.withItemsList
            .partition { it.subSelect.selectBody is SetOperationList } // Union or not
            .let { (cte, nonUnion) ->
                nonUnion.forEach { withSelectMap[it.name] = arrayListOf(it.subSelect.selectBody as PlainSelect) }
                cte
            }
            .forEach { cte ->
                (cte.subSelect.selectBody as SetOperationList).let { union ->
                    val tables = ArrayList<String>()
                    val selects = ArrayList<PlainSelect>()

                    /** Recursive, Self-contained */
                    val statementSplit = Pair<ArrayList<PlainSelect>, ArrayList<PlainSelect>>(ArrayList(), ArrayList())
                    val aliases = arrayListOf(cte.name)
                    var crossCTE = false
                    var recursive: Boolean
                    fun rateTable(table: FromItem) {
                        if (table is Table) {
                            when (table.name) {
                                cte.name -> recursive = true // recursive
                                in cteNames -> crossCTE = true // dependant on other cte
                                else -> { // should be normal table
                                    aliases.add(table.alias?.name ?: table.name)
                                    if (table.name !in tables) {
                                        tables.add(table.name)
                                    }
                                }
                            }
                        } else {
                            unknown(query, "Nested SubSelects not supported for CTE")
                        }
                    }

                    union.selects.forEach { subSelect ->
                        recursive = false
                        if (subSelect is PlainSelect) {
                            subSelect.fromItem.run { rateTable(this) }
                            subSelect.joins?.forEach { rateTable(it.rightItem) }
                            if (recursive) {
                                statementSplit.first.add(subSelect)
                            } else {
                                statementSplit.second.add(subSelect)
                            }
                        } else {
                            unknown(query, "Nested SubSelects not supported for CTE")
                        }
                        if (recursive && crossCTE) {
                            unknown(query, "Does not support both recursive and referencing other CTE in same CTE")
                        }
                    }
                    if (crossCTE) { // CTEs should not be recursive and reference other, either or
                        selects.addAll(union.selects.filterIsInstance<PlainSelect>())
                    } else if (tables.size == 1) {
                        statementSplit.second.forEach { base ->
                            (base.fromItem as Table).let { table ->
                                table.alias = Alias(cte.name)
                                base.selectItems.forEach { it.accept((TableReplacerFactory(table, aliases))) }
                            }
                            val tableFixer = TableFixerFactory(base, cte.name)
                            statementSplit.first.forEach { stmt ->
                                applyVisitor(stmt, tableFixer)
                                selects.add(stmt)
                            }
                        }
                    } else {
                        // ??
                        unknown(query)
                    }
                    withSelectMap[cte.name] = selects
                }
            }

        cteNames.forEach { with ->
            val tempList = ArrayList<ArrayList<WithItem>>()
            out.forEach { temp ->
                withSelectMap[with]?.forEach { splits ->
                    tempList.add(ArrayList<WithItem>(temp).also {
                        withMap[with]?.clone()?.withSubSelect(SubSelect().withSelectBody(splits))?.run { it.add(this) }
                    })
                }
            }
            out = tempList
        }
        return out.map { query.clone().withWithItemsList(it) }
    }
    return arrayListOf(query)
}

/**
 * Splits a SQL query on UNIONs in JOIN
 */
fun cleanUnionJoin(query: Select): List<Select> {
    // TODO Not implemented
    return arrayListOf(query)
}

/**
 * Splits a SQL query on UNIONs in condition
 */
fun cleanUnionCondition(query: Select): List<Select> {
    // TODO Not implemented
    return arrayListOf(query)
}

fun cleanSubSelect(body: PlainSelect) {
    fun clean(from: FromItem, index: Int = 0): FromItem? {
        if (from is SubSelect) {
            from.selectBody.let { subSelect ->
                if (subSelect is PlainSelect) {
                    val tableFixer = TableFixerFactory(subSelect, from.alias?.name)
                    /*
                    // Need to get table name from subSelect in case no explicit table name is used in subSelect items
                    val table = subSelect.fromItem.let { maybeTable ->
                        if (maybeTable is Table) {
                            maybeTable
                        } else {
                            Table(maybeTable.alias.name)
                        }
                    }
                    val tableFixer = object : ExpressionVisitorAdapter() {
                        override fun visit(column: Column?) {
                            if (column != null) {
                                when {
                                    column.table == null -> column.table = table
                                    column.table.name == from.alias?.name ->
                                        subSelect.selectItems
                                            .filterIsInstance<SelectExpressionItem>()
                                            .find { subItem ->
                                                column.columnName == (subItem.alias?.name ?: if (subItem.expression is Column) {
                                                    (subItem.expression as Column).columnName
                                                } else {
                                                    null
                                                })
                                            }
                                            ?.also { subItem -> // Replace Item with item from subSelect
                                                subItem.expression.let { expr ->
                                                    if (expr is Column && expr.table == null) {
                                                        expr.table = table
                                                    }
                                                }
                                                when (val item = getParent(column.astNode)) {
                                                    null -> {}
                                                    is SelectExpressionItem -> {
                                                        item.expression = subItem.expression
                                                        if (item.expression !is Column) { // Replaced Column from subSelect with non-column, alias needed
                                                            item.alias = subItem.alias
                                                        }
                                                    }
                                                    is BinaryExpression -> {
                                                        fun checkColumn(expr: Expression) =
                                                            expr is Column && expr.table.name == from.alias.name
                                                        when {
                                                            checkColumn(item.leftExpression) -> item.leftExpression = subItem.expression
                                                            checkColumn(item.rightExpression) -> item.rightExpression = subItem.expression
                                                        }
                                                    }
                                                    else -> println()
                                                }
                                            }
                                }
                            }
                        }
                    }
                    tableFixer.selectVisitor = object : SelectVisitorAdapter() {
                        override fun visit(plainSelect: PlainSelect?) {
                            plainSelect?.where?.accept(tableFixer)
                        }
                    }*/

                    // Fix select items
                    body.selectItems.forEach { item -> item.accept(tableFixer) }
                    // Add joins
                    if (subSelect.joins != null) {
                        body.joins = (body.joins ?: arrayListOf())
                            .withIndex()
                            .partition { it.index < index }
                            .let { (before, after) -> ArrayList(before.map { it.value } + subSelect.joins + after.map { it.value }) }
                    }
                    body.joins?.forEach { join ->
                        join.onExpressions.forEach { on -> on.accept(tableFixer) }
                        join.usingColumns.forEach { on -> on.accept(tableFixer) }
                    }
                    // Add conditions
                    if (body.where == null) {
                        body.where = subSelect.where
                        body.where.accept(tableFixer)
                    } else {
                        body.where // TODO Don't know how to handle this, had no example
                    }
                    return subSelect.fromItem
                }
            }
        }
        return null
    }
    body.fromItem.let { from ->
        clean(from)?.let {
            // Fix TableName
            body.fromItem = it
        }
    }
    body.joins?.forEachIndexed { i, join ->
        clean(join.rightItem, i + 1)?.let {
            // Fix TableName
            join.rightItem = it
        }
    }
    // TODO Replace Joins with subselect
}

fun cleanSubSelect(select: Select): Select {
    select.selectBody.let { body ->
        if (body is PlainSelect) {
            cleanSubSelect(body)
        }
    }
    return select
}

fun cleanWith(query: Select): Select {
    if (query.withItemsList?.isNotEmpty() == true) {
        // TODO Merge WithItems into single
        val withMap = query.withItemsList.associateBy { it.name }
        val ctes = withMap.keys
        val dependencies = HashMap<String, MutableList<String>>()
        val dependants = ArrayList<String>()
        fun addDeps(from: String, to: String) {
            if (from != to && to in ctes) {
                if (dependencies[from] == null) {
                    dependencies[from] = ArrayList()
                }
                dependencies[from]!!.add(to)
                if (to !in dependants) {
                    dependants.add(to)
                }
            }
        }

        fun replaceWithSubSelect(name: String) = SubSelect().withSelectBody(withMap[name]?.subSelect?.selectBody)

        query.withItemsList.forEach { with ->
            (with.subSelect.selectBody as PlainSelect).let { select ->
                addDeps(with.name, (select.fromItem as Table).name)
                select.joins?.forEach { addDeps(with.name, (it.rightItem as Table).name) }
            }
        }

        // reduce from root recursive
        fun reduceCTE(cte: String) {
            dependencies[cte]?.forEach { reduceCTE(it) }
            withMap[cte]?.let { with ->
                replaceTable((with.subSelect?.selectBody as PlainSelect), ::replaceWithSubSelect, Alias(cte)) { it in ctes && it != cte }
            }
        }

        val root = dependencies.keys.filter { it !in dependants }
        root.forEach { reduceCTE(it) }

        // TODO integrate WITH select into query
        (query.selectBody as PlainSelect).let { body ->
            replaceTable(body, ::replaceWithSubSelect, body.fromItem.alias) { it in ctes }
        }
        query.withItemsList = null
    }
    return query
}

fun replaceTable(base: PlainSelect, subSelectFor: (String) -> SubSelect, alias: Alias?, replacementFilter: (String) -> Boolean = { true }) {
    (base.fromItem as Table).let { table ->
        if (replacementFilter(table.name)) {
            base.fromItem = subSelectFor(table.name).withAlias(alias)
        }
    }
    base.joins?.forEach {
        (it.rightItem as Table).let { table ->
            if (replacementFilter(table.name)) {
                it.rightItem = subSelectFor(table.name).withAlias(table.alias ?: Alias(table.name))
            }
        }
    }
    cleanSubSelect(base)
}

/**
 * Factory for visitor to replace the tables in the alias list with the given table.
 *
 * @param table The table to replace the old table with
 * @param alias List of aliases to replace
 */
fun TableReplacerFactory(table: Table, alias: List<String?>): ExpressionVisitorAdapter {
    val tableFixer = object : ExpressionVisitorAdapter() {
        override fun visit(column: Column?) {
            if (column?.table?.name in alias) {
                column!!.table = table
            }
        }
    }
    tableFixer.selectVisitor = object : SelectVisitorAdapter() {
        override fun visit(plainSelect: PlainSelect?) {
            plainSelect?.where?.accept(tableFixer)
        }
    }
    return tableFixer
}

/**
 * Factory for visitor to pull up subselect in referenced queries
 *
 * @param select The select query to pull up
 * @param alias Optional: The alias the subselect is referenced by, if applicable
 */
fun TableFixerFactory(select: PlainSelect, alias: String?): ExpressionVisitorAdapter {
    // Need to get table name from subSelect in case no explicit table name is used in subSelect items
    val table = select.fromItem.let { maybeTable ->
        if (maybeTable is Table) {
            maybeTable
        } else {
            Table(maybeTable.alias.name)
        }
    }

    fun checkColumn(expr: Expression) = expr is Column && expr.table.name == alias
    val tableFixer = object : ExpressionVisitorAdapter() {
        override fun visit(column: Column?) {
            if (column != null) {
                when {
                    column.table == null -> column.table = table
                    checkColumn(column) -> select.selectItems.filterIsInstance<SelectExpressionItem>().find { subItem ->
                        column.columnName == (subItem.alias?.name ?: if (subItem.expression is Column) {
                            (subItem.expression as Column).columnName
                        } else {
                            null
                        })
                    }?.also { subItem -> // Replace Item with item from subSelect
                        subItem.expression.let { expr ->
                            if (expr is Column && expr.table == null) {
                                expr.table = table
                            }
                        }
                        when (val item = getParent(column.astNode)) {
                            null -> {}
                            is SelectExpressionItem -> {
                                item.expression = subItem.expression
                                if (item.expression !is Column) { // Replaced Column from subSelect with non-column, alias needed
                                    item.alias = subItem.alias
                                }
                            }
                            is BinaryExpression -> {
                                when {
                                    checkColumn(item.leftExpression) -> item.leftExpression = subItem.expression
                                    checkColumn(item.rightExpression) -> item.rightExpression = subItem.expression
                                }
                            }
                            else -> println()
                        }
                    }
                }
            }
        }
    }
    tableFixer.selectVisitor = object : SelectVisitorAdapter() {
        override fun visit(plainSelect: PlainSelect?) {
            plainSelect?.where?.accept(tableFixer)
        }
    }
    return tableFixer
}

fun applyVisitor(select: PlainSelect, visitor: ExpressionVisitorAdapter) {
    select.selectItems?.forEach { it.accept(visitor) }
    select.joins?.forEach { join ->
        join.onExpressions.forEach { it.accept(visitor) }
        join.usingColumns.forEach { it.accept(visitor) }
    }
    select.where?.accept(visitor)
}

fun getParent(node: SimpleNode): Any? {
    val parent = node.jjtGetParent()
    if (parent is SimpleNode) {
        return parent.jjtGetValue() ?: getParent(parent)
    }
    return null
}

fun parseSelect(select: PlainSelect): Collection<QueryStructure.Table> {
    val tables = HashMap<String, QueryStructure.Table>()
    val aliases = HashMap<String, String>()
    var defaultTable: Table? = null
    val tableOrder = ArrayList<String>()
    fun addColumn(column: Column) {
        if (column.table == null && defaultTable != null) {
            column.table = defaultTable
        }
        (aliases[column.table?.name] ?: column.table?.name)?.let { name ->
            if (tables[name] == null) {
                tables[name] = QueryStructure.Table(name)
                tableOrder.add(name)
            }
            tables[name]!!.Properties.let {
                if (column.columnName !in it) {
                    it.add(column.columnName)
                }
            }
        }
    }

    fun addRelation(relation: Pair<Column, Column>, table: String) {
        val (first, second) = relation
        val current: Column
        val other: Column
        if ((aliases[first.table.name] ?: first.table.name) == table) {
            current = first
            other = second
        } else {
            current = second
            other = first
        }
        tables[aliases[other.table.name] ?: other.table.name]?.Relations?.let { tableRelations ->
            if (!tableRelations.contains(table)) {
                tableRelations[table] = ArrayList()
            }
            tableRelations[table]!!.let { tableRelation ->
                if (tableRelation.find { it.first == current.columnName && it.second == other.columnName } == null) {
                    tableRelation.add(Pair(other.columnName, current.columnName))
                }
            }
        }
    }

    // get base table
    select.fromItem.let { from ->
        if (from is Table) {
            tables[from.name] = QueryStructure.Table(from.name)
            aliases[from.alias?.name ?: from.name] = from.name
            defaultTable = from
        } else {
            unknown(Select().withSelectBody(select), "Unknown FROM")
        }
    }
    // get tables, props and relations from join
    select.joins?.forEach { join ->
        join.rightItem.let { from ->
            if (from is Table) {
                if (tables[from.name] == null) {
                    tables[from.name] = QueryStructure.Table(from.name)
                    aliases[from.alias?.name ?: from.name] = from.name
                }
                join.onExpressions?.forEach { on ->
                    val (columns, relations) = parseExpression(on)
                    columns.forEach(::addColumn)
                    relations.forEach { addRelation(it, from.name) }
                }
            } else {
                unknown(Select().withSelectBody(select), "Unknown JOIN: $join")
            }
        }
    }
    // get Props from select
    select.selectItems.forEach { item ->
        item.accept(object : ExpressionVisitorAdapter() {
            override fun visit(column: Column?) {
                if (column != null) {
                    addColumn(column)
                }
            }
        })
    }
    // TODO get Props from where
    if (select.where != null) {
        val (columns, relations) = parseExpression(select.where, defaultTable)
        columns.forEach(::addColumn)
        relations.forEach {
            addRelation(it, tableOrder.first { table -> it.first.table.name == table || it.second.table.name == table })
        }
    }
    // TODO get Props from orderby
    return tables.values
}

/**
 * Parses an Expression to return contained Columns and Relations between tables with column
 */
fun parseExpression(expression: Expression, default: Table? = null): Pair<List<Column>, List<Pair<Column, Column>>> {
    val columns = ArrayList<Column>()
    val relations = ArrayList<Pair<Column, Column>>()
    val aliases = HashMap<String, String>()
    var defaultTable: Table? = default
    var otherSide: Column? = null

    fun fixTable(column: Column) {
        if (column.table == null && defaultTable != null) {
            column.table = defaultTable
        }
        if (aliases[column.table.name] != null) {
            column.table.name = aliases[column.table.name]
        }
    }

    fun add(left: Column, right: Column) {
        fixTable(right)
        fixTable(left)
        if (relations.find { (r, l) -> (left.similar(l) && right.similar(r)) || (left.similar(r) && right.similar(l)) } == null && left.table.name != right.table.name) {
            relations.add(Pair(right, left))
        }
    }

    fun handleInnerSelect(select: PlainSelect) {
        select.fromItem.let { from ->
            if (from is Table) {
                aliases[from.alias?.name ?: from.name] = from.name
                defaultTable = from
                if (otherSide != null && select.selectItems.size > 0) {
                    select.selectItems[0].let { item ->
                        if (item is SelectExpressionItem && item.expression is Column) {
                            add(otherSide as Column, item.expression as Column)
                        }
                    }
                }
            }
        }
    }

    val visitor = object : ExpressionVisitorAdapter() {
        override fun visit(column: Column?) {
            if (column != null) {
                fixTable(column)
                if (columns.find { column.similar(it) } == null) {
                    columns.add(column)
                }
            }
        }

        override fun visit(expr: EqualsTo?) {
            if (expr != null) {
                check(expr.leftExpression, expr.rightExpression)
            }
            super.visit(expr)
        }

        override fun visit(expr: InExpression?) {
            if (expr != null && expr.rightExpression != null) {
                check(expr.leftExpression, expr.rightExpression)
            }
            super.visit(expr)
        }

        fun check(left: Expression, right: Expression) {
            fun getParen(expr: Expression): Expression = if (expr is Parenthesis) {
                getParen(expr.expression)
            } else {
                expr
            }

            val leftNP = getParen(left)
            val rightNP = getParen(right)
            when {
                leftNP is Column && rightNP is Column -> {
                    add(leftNP, rightNP)
                }
                leftNP is Column && (rightNP is Function || rightNP is SubSelect) -> {
                    otherSide = leftNP
                }
                rightNP is Column && (leftNP is Function || leftNP is SubSelect) -> {
                    otherSide = rightNP
                }
            }
        }

        override fun visit(function: Function?) {
            if (function?.name?.lowercase(Locale.getDefault()) == "coalesce" && (function.parameters?.expressions?.size ?: 0) > 0) {
                function.parameters.expressions[0].let { param ->
                    when (param) {
                        is SubSelect -> {
                            val oldDefault = defaultTable
                            param.selectBody.let { select ->
                                if (select is PlainSelect) {
                                    handleInnerSelect(select)
                                }
                            }
                            visit(param)
                            defaultTable = oldDefault
                        }
                        is Column -> visit(param)
                        else -> super.visit(function)
                    }
                }
            } else {
                super.visit(function)
            }
        }

    }
    visitor.selectVisitor = object : SelectVisitorAdapter() {
        override fun visit(plainSelect: PlainSelect?) {
            val oldDefault = defaultTable
            if (plainSelect != null) {
                handleInnerSelect(plainSelect)
                plainSelect.selectItems?.forEach { it.accept(visitor) }
                plainSelect.where?.accept(visitor)
            }
            defaultTable = oldDefault
        }
    }
    expression.accept(visitor)
    return Pair(columns, relations)
}

fun unknown(query: Statement, reason: String? = null) {
    println("Don't Know how to handle SQL. ${reason ?: ""}\n$query")
}

fun createSchemaDesign(queries: List<QueryStructure>) {
    /*
    Input:
    var userTables- A list of relational database tables with table structure,
    var relationArrayList –A list of relationship of relational tables(Format given in Table 1),
    var selectQueryJSON –A list select queries in JSON file (Format given in Figure 2(a)),
    var updateQueryJSON –A list of update queries in JSON file (Format given in Figure 2(b)).
    Output: All possible combination of each relational tables in JSON format, after applying select and update queries, in
    tableList[][] for document store database of NoSQL.
    Begin
    for each table x in userTables, create t document as a JSON structure
    Begin
    var STab[] //It is a one dimensional array on JSON object.
    //Now t JSON has key/value corresponding to columnName/dataType of x table.
    Add updateWeight=0 and selectWeight= 0 to t as key/Value.
    Add t in STab[].
    for each relation rs in relationArrayList
    Begin
    If t and keys of t is available in (p_table, p_column) of relationArrayList
    Begin
    for each(t.key and corresponding elements of list[][] are in select-list of each selectQueryJSON query )
    Begin
    1. Add/ update each select queries select-list with intermediate table’s primary key in t{} as embed with c_table as
    keyName (c_table of Table 1).
    2. Add/update Where clauses column-tablename in t{} as embed with c_table as keyName (Only Child Table Columns
    will be added/updated in Parent table).
    3. Update selectWeight = selectWeight+CurrentSelectQueryWeight in t{}.
    4. if Adding select-list data to t{} with those column that are in update query of updateQueryJSON (update columns
    must be of different tables then t{}) then Update updateWeight = updateWeight+CurrentUpdateQueryWeight in t{}.
    5. For each updated t{}, Updated t{} can be renamed to tQueryID and add tQueryID in list STab[].
    End for;
    End if;
    End for;
    Add STab[] in tableList[][]
    End for;
    End Algorithm 1.
     */
}