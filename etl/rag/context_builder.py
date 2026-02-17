"""RAG Context Building Component"""

from typing import Dict, List


def build_context(retrieved_docs: List[Dict], max_chars: int = 12000) -> str:
    """Build context string from retrieved documents."""
    context_parts = []
    total_chars = 0

    for i, doc in enumerate(retrieved_docs, 1):
        doc_context = f"""
--- DATASET {i} (Relevance: {doc['relevance_score']:.0%}) ---
Title: {doc['title']}
Abstract: {doc['abstract']}
Keywords: {', '.join(doc['keywords']) if doc['keywords'] else 'None'}
"""
        if doc.get("extracted_text"):
            doc_context += f"Content: {doc['extracted_text'][:500]}...\n"

        if total_chars + len(doc_context) > max_chars:
            break

        context_parts.append(doc_context)
        total_chars += len(doc_context)

    return "\n".join(context_parts)
