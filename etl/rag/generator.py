"""RAG Generation Component"""

from typing import Any, Dict

import httpx

SYSTEM_PROMPT = """You are a research assistant helping users discover environmental datasets.
Based on the provided dataset information, answer the user's question.
Always cite which dataset(s) your answer is based on using [Dataset N] format.
If the datasets don't contain relevant information, say so clearly."""


async def generate_answer(
    question: str,
    context: str,
    ollama_url: str = "http://localhost:11434",
    model: str = "phi3:mini",
    timeout: float = 60.0,
) -> Dict[str, Any]:
    """Generate answer using Ollama, with fallback."""

    prompt = f"""{SYSTEM_PROMPT}

AVAILABLE DATASETS:
{context}

USER QUESTION: {question}

Answer based on the datasets above, citing dataset numbers."""

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{ollama_url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.7},
                },
            )

            if response.status_code == 200:
                data = response.json()
                return {
                    "answer": data.get("response", ""),
                    "model": model,
                    "generated": True,
                }
    except Exception as e:
        print(f"Ollama unavailable: {e}")

    # Fallback
    return {
        "answer": (
            f"Based on my search, I found these relevant datasets:\n\n"
            f"{context}\n\n"
            f"Review the datasets above for detailed information."
        ),
        "model": "fallback",
        "generated": False,
    }
