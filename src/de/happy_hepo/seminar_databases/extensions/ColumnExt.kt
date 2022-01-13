package de.happy_hepo.seminar_databases.extensions

import net.sf.jsqlparser.schema.Column

fun Column.similar(other:Column) = this.table.name == other.table.name && this.columnName == other.columnName