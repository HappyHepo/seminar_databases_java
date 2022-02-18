package de.happy_hepo.seminar_databases.ui

import de.happy_hepo.seminar_databases.DocumentStructure

class CliUi : IUi {
    private inline fun <reified T : Enum<T>> ask(): T {
        println("Options:")
        println(enumValues<T>().joinToString("\n"))
        var result: T? = null
        do {
            try {
                val answer = readln().toInt()
                result = enumValues<T>().getOrNull(answer)
            } catch (_: NumberFormatException) {
            }
            if (result == null) {
                println("Please enter a valid option from above!")
            }
        } while (result == null)
        return result
    }

    private fun ask(options: Set<String>): String {
        println("Options:")
        println(options.mapIndexed { i, option -> "$i $option" }.joinToString("\n"))
        var result: String? = null
        do {
            try {
                val answer = readln().toInt()
                result = options.elementAtOrNull(answer)
            } catch (_: NumberFormatException) {
            }
            if (result == null) {
                println("Please enter a valid option from above!")
            }
        } while (result == null)
        return result
    }

    override fun askApproval(document: DocumentStructure): ApprovalAnswer {
        println("Please review the following document:")
        println(document.toString(2) + "\n")
        val response = ask<ApprovalAnswer>()
        fun getDocsWithRelations(document: DocumentStructure): Set<String> {
            val out = mutableSetOf(document.name)
            out.addAll(document.nested
                .filter { it.nested.size > 0 }
                .flatMap { getDocsWithRelations(it) })
            return out
        }

        fun getDocByName(name: String, document: DocumentStructure): DocumentStructure? {
            return if (document.name == name) {
                document
            } else {
                document.nested.find { getDocByName(name, it) != null }
            }
        }
        when (response) {
            ApprovalAnswer.Reverse -> { // Get Reverse Path
                println("\nFrom table:")
                var from = ask(getDocsWithRelations(document))
                val data = mutableListOf(from)
                var target: String?
                do {
                    println("\nTo table (0 to end):")
                    target = getDocByName(from, document)?.let { doc ->
                        ask(setOf("", *doc.nested.map { it.name }.toTypedArray())).let { it.ifEmpty { null } }
                    }
                    if (target != null) {
                        data.add(target)
                        from = target
                    }
                } while (target != null)
                if (data.size > 1) {
                    response.data = data
                }
            }
            ApprovalAnswer.Split, ApprovalAnswer.Duplicate -> { // Get Split Point
                println("\nFrom table:")
                val from = ask(getDocsWithRelations(document))
                println("\nSplit child table:")
                val target = getDocByName(from, document)!!.let { doc ->
                    if (doc.nested.size == 1) {
                        doc.nested[0].name
                    } else {
                        ask(doc.nested.map { it.name }.toSet())
                    }
                }
                response.data = listOf(from, target)
            }
            else -> {}
        }
        return response
    }
}