package de.happy_hepo.seminar_databases.ui

import de.happy_hepo.seminar_databases.DocumentStructure

enum class ApprovalAnswer(
    private val description: String,
    var data: List<String>? = null,
) {
    Approve("Accept the document structure as-is.\n  No further approval will be asked for this document."),
    Reverse("Reverse part of the hierarchy."),
    Split("Split a nested document into separate document."),
    Duplicate("Duplicate part of the nested document into separate document.");

    override fun toString(): String {
        return "${this.ordinal} ${this.description}"
    }
}

interface IUi {
    fun askApproval(document: DocumentStructure): ApprovalAnswer
}