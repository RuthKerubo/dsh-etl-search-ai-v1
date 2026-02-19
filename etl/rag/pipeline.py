"""Main RAG Pipeline with query intent detection and guardrails."""

import re
from typing import Any, Dict, Optional

from .context_builder import build_context
from .generator import generate_answer
from .retriever import DatasetRetriever
from etl.guardrails import DataGuardrails, RAGGuardrails


class QueryClassifier:
    """Classify query intent to handle non-search queries appropriately."""

    GREETING_PATTERNS = [
        r'^(hi|hello|hey|greetings|good morning|good afternoon|good evening)[\s!?,.:]*$',
        r'^(how are you|whats up|what\'s up|sup)[\s!?,.:]*$',
        r'^(yo|hiya|howdy)[\s!?,.:]*$',
    ]

    HELP_PATTERNS = [
        r'^(help|how do i|how to use|what can you do|how does this work)[\s!?,.:]*',
        r'^(what is this|explain|guide me)[\s!?,.:]*',
    ]

    ABOUT_PATTERNS = [
        r'^(who are you|what are you|tell me about yourself|about)[\s!?,.:]*$',
        r'^(what is this system|what is dsh)[\s!?,.:]*',
    ]

    NONSENSE_PATTERNS = [
        r'^[^a-zA-Z0-9]*$',  # Only symbols
        r'^(.)\1{3,}$',      # Repeated characters like "aaaa"
    ]

    @classmethod
    def classify(cls, query: str) -> Dict[str, Any]:
        """Classify the query and return intent with appropriate response."""
        query_clean = query.strip()
        query_lower = query_clean.lower()

        # Empty or too short
        if len(query_clean) < 2:
            return {
                "intent": "too_short",
                "is_search": False,
                "response": "Please enter a search query. You can ask about topics like 'water quality', 'soil carbon', or 'climate data'."
            }

        # Nonsense patterns
        for pattern in cls.NONSENSE_PATTERNS:
            if re.match(pattern, query_lower):
                return {
                    "intent": "nonsense",
                    "is_search": False,
                    "response": "I didn't understand that. Try searching for environmental topics like 'river water quality' or 'land cover mapping'."
                }

        # Greetings
        for pattern in cls.GREETING_PATTERNS:
            if re.match(pattern, query_lower):
                return {
                    "intent": "greeting",
                    "is_search": False,
                    "response": "Hello! I can help you find UK environmental datasets. Try asking about:\n\n• Water quality monitoring data\n• Soil and land cover surveys\n• Climate and weather records\n• Biodiversity observations\n\nWhat topic are you interested in?"
                }

        # Help requests
        for pattern in cls.HELP_PATTERNS:
            if re.match(pattern, query_lower):
                return {
                    "intent": "help",
                    "is_search": False,
                    "response": "This system searches the UKCEH environmental dataset catalogue. You can:\n\n• Ask questions like 'What water quality data is available?'\n• Search for topics: 'soil carbon UK peatlands'\n• Find specific data: 'river Thames monitoring'\n\nResults are ranked using hybrid search (keywords + semantic similarity)."
                }

        # About/identity questions
        for pattern in cls.ABOUT_PATTERNS:
            if re.match(pattern, query_lower):
                return {
                    "intent": "about",
                    "is_search": False,
                    "response": "I'm a search assistant for the DSH Environmental Dataset catalogue. I help you find UK environmental datasets from the UKCEH Environmental Data Centre.\n\nThe system uses hybrid search combining keyword matching with semantic similarity to find relevant datasets."
                }

        # Single common words that aren't searches
        common_non_search = ['yes', 'no', 'ok', 'okay', 'thanks', 'thank you', 'bye', 'goodbye', 'cool', 'nice', 'great']
        if query_lower in common_non_search:
            return {
                "intent": "acknowledgement",
                "is_search": False,
                "response": "Is there anything else you'd like to search for? You can ask about environmental topics like water quality, soil data, climate records, or biodiversity."
            }

        # Default: treat as a valid search query
        return {
            "intent": "search",
            "is_search": True,
            "response": None
        }


class RAGPipeline:
    """RAG pipeline with intent detection and access control."""

    def __init__(self, collection, embedding_model):
        self.retriever = DatasetRetriever(collection, embedding_model)

    async def query(
        self,
        question: str,
        top_k: int = 5,
        min_relevance: float = 0.3,
        use_llm: bool = True,
        user_role: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Full RAG pipeline with guardrails.
        
        Steps:
        1. Classify query intent (handle greetings, help, off-topic)
        2. Retrieve relevant datasets
        3. Filter by user access level
        4. Build context from results
        5. Generate answer (LLM or fallback)
        6. Redact PII from response
        """

        # 1. Classify query intent
        classification = QueryClassifier.classify(question)

        if not classification["is_search"]:
            return {
                "question": question,
                "answer": classification["response"],
                "sources": [],
                "generated": False,
                "model": "intent_classifier",
            }

        # 2. Retrieve relevant datasets
        retrieved = await self.retriever.retrieve(
            query=question,
            top_k=top_k,
            min_score=min_relevance,
        )

        # 3. Filter by access level
        retrieved = RAGGuardrails.filter_context_by_access(retrieved, user_role)

        if not retrieved:
            return {
                "question": question,
                "answer": "No relevant datasets found for your query. Try different keywords or broader terms.\n\nExample searches:\n• 'water quality monitoring'\n• 'soil carbon data'\n• 'UK climate observations'",
                "sources": [],
                "generated": False,
                "model": None,
            }

        # 4. Build context
        context = build_context(retrieved)

        # 5. Generate answer
        if use_llm:
            generation = await generate_answer(question, context)
        else:
            # Format results nicely for fallback mode
            result_text = f"Found {len(retrieved)} relevant datasets:\n\n"
            for i, d in enumerate(retrieved, 1):
                score_pct = int(d.get('relevance_score', 0) * 100)
                result_text += f"**{i}. {d.get('title', 'Untitled')}** (Relevance: {score_pct}%)\n"
                abstract = d.get('abstract', '')
                if abstract:
                    # Truncate long abstracts
                    if len(abstract) > 200:
                        abstract = abstract[:200] + "..."
                    result_text += f"{abstract}\n"
                result_text += "\n"

            generation = {
                "answer": result_text,
                "model": "fallback",
                "generated": False,
            }

        # 6. Redact PII from answer
        validated = RAGGuardrails.validate_response(generation["answer"], user_role)
        generation["answer"] = validated["response"]

        return {
            "question": question,
            "answer": generation["answer"],
            "sources": [
                {
                    "id": d.get("id", d.get("identifier", "")),
                    "title": d.get("title", "Untitled"),
                    "relevance_score": d.get("relevance_score", 0),
                }
                for d in retrieved
            ],
            "generated": generation.get("generated", False),
            "model": generation.get("model"),
        }