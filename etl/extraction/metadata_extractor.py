"""
Metadata extraction from PDF text.

Primary: Ollama LLM extraction (structured JSON output).
Fallback: Rule-based extraction with TF-IDF keywords.
"""

import json
import os
import re

import httpx


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi3:mini")

# Keyword-to-ISO-19115 topic category mapping
KEYWORD_CATEGORY_MAP = {
    "biodiversity": "biota",
    "species": "biota",
    "habitat": "biota",
    "ecology": "biota",
    "wildlife": "biota",
    "vegetation": "biota",
    "flora": "biota",
    "fauna": "biota",
    "climate": "climatologyMeteorologyAtmosphere",
    "temperature": "climatologyMeteorologyAtmosphere",
    "rainfall": "climatologyMeteorologyAtmosphere",
    "precipitation": "climatologyMeteorologyAtmosphere",
    "weather": "climatologyMeteorologyAtmosphere",
    "atmospheric": "climatologyMeteorologyAtmosphere",
    "water": "inlandWaters",
    "river": "inlandWaters",
    "lake": "inlandWaters",
    "hydrology": "inlandWaters",
    "groundwater": "inlandWaters",
    "catchment": "inlandWaters",
    "ocean": "oceans",
    "marine": "oceans",
    "coastal": "oceans",
    "sea": "oceans",
    "soil": "geoscientificInformation",
    "geology": "geoscientificInformation",
    "geochemistry": "geoscientificInformation",
    "sediment": "geoscientificInformation",
    "elevation": "elevation",
    "topography": "elevation",
    "terrain": "elevation",
    "land cover": "imageryBaseMapsEarthCover",
    "land use": "imageryBaseMapsEarthCover",
    "satellite": "imageryBaseMapsEarthCover",
    "remote sensing": "imageryBaseMapsEarthCover",
    "farming": "farming",
    "agriculture": "farming",
    "crop": "farming",
    "livestock": "farming",
    "pollution": "environment",
    "emission": "environment",
    "environmental": "environment",
    "conservation": "environment",
}

OLLAMA_PROMPT = """Extract metadata from this scientific document text. Return valid JSON with these fields:
- "title": the document title (string)
- "abstract": a 1-3 sentence summary (string)
- "keywords": up to 10 relevant keywords (array of strings)
- "topic_categories": ISO 19115 topic categories that apply (array of strings, choose from: farming, biota, boundaries, climatologyMeteorologyAtmosphere, economy, elevation, environment, geoscientificInformation, health, imageryBaseMapsEarthCover, inlandWaters, location, oceans, planningCadastre, society, structure, transportation, utilitiesCommunication)

Document text:
{text}"""


class MetadataExtractor:
    """Extract metadata from document text using Ollama or rule-based fallback."""

    async def extract(self, text: str) -> dict:
        """
        Extract metadata from text.

        Tries Ollama first, falls back to rule-based extraction.
        """
        result = await self._extract_ollama(text)
        if result is not None:
            return result
        return self._extract_fallback(text)

    async def _extract_ollama(self, text: str) -> dict | None:
        """Extract metadata using Ollama LLM."""
        truncated = text[:3000]
        prompt = OLLAMA_PROMPT.format(text=truncated)

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={
                        "model": OLLAMA_MODEL,
                        "prompt": prompt,
                        "format": "json",
                        "stream": False,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                parsed = json.loads(data["response"])

                return {
                    "title": parsed.get("title", "").strip() or None,
                    "abstract": parsed.get("abstract", "").strip() or None,
                    "keywords": [k for k in parsed.get("keywords", []) if isinstance(k, str) and k.strip()],
                    "topic_categories": [c for c in parsed.get("topic_categories", []) if isinstance(c, str) and c.strip()],
                }
        except Exception:
            return None

    def _extract_fallback(self, text: str) -> dict:
        """Rule-based metadata extraction with TF-IDF keywords."""
        title = self._extract_title(text)
        abstract = text[:500].strip()
        keywords = self._extract_keywords_tfidf(text)
        topic_categories = self._infer_categories(text)

        return {
            "title": title,
            "abstract": abstract,
            "keywords": keywords,
            "topic_categories": topic_categories,
        }

    @staticmethod
    def _extract_title(text: str) -> str:
        """Use first non-empty line as title."""
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                # Cap at 200 chars
                return stripped[:200]
        return "Untitled Document"

    @staticmethod
    def _extract_keywords_tfidf(text: str, top_n: int = 10) -> list[str]:
        """Extract keywords using TF-IDF."""
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer

            # Split text into pseudo-documents (paragraphs)
            paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
            if not paragraphs:
                return []

            vectorizer = TfidfVectorizer(
                max_features=50,
                stop_words="english",
                ngram_range=(1, 2),
                min_df=1,
            )
            tfidf_matrix = vectorizer.fit_transform(paragraphs)
            feature_names = vectorizer.get_feature_names_out()

            # Sum TF-IDF scores across paragraphs
            scores = tfidf_matrix.sum(axis=0).A1
            ranked = sorted(zip(feature_names, scores), key=lambda x: x[1], reverse=True)
            return [term for term, _ in ranked[:top_n]]
        except Exception:
            return []

    @staticmethod
    def _infer_categories(text: str) -> list[str]:
        """Map keywords in text to ISO 19115 topic categories."""
        text_lower = text.lower()
        categories = set()
        for keyword, category in KEYWORD_CATEGORY_MAP.items():
            if keyword in text_lower:
                categories.add(category)
        return sorted(categories)
