package de.happy_hepo.seminar_databases.extensions

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select
import net.sf.jsqlparser.statement.select.WithItem

fun WithItem.clone(): WithItem {
    return (CCJSqlParserUtil.parse("WITH $this SELECT 1;") as Select).withItemsList[0]
}