package de.happy_hepo.seminar_databases.extensions

import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.select.Select

fun Select.clone(): Select {
    return CCJSqlParserUtil.parse(this.toString()) as Select
}