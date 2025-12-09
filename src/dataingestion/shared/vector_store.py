import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class SimpleVectorStore:
    """
    In-memory keyword-based retriever (MVP for RAG).
    """

    def __init__(self):
        self.index: List[Dict[str, Any]] = []

    def add_documents(self, chunks: List[Dict[str, str]], metadata: Dict[str, Any]):
        """
        Add chunks to the index.
        """
        for chunk in chunks:
            self.index.append(
                {"text": chunk["text"], "index": chunk["index"], "metadata": metadata}
            )

    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Simple keyword search.
        """
        query_terms = query.lower().split()
        results = []

        for doc in self.index:
            score = 0
            text_lower = doc["text"].lower()

            for term in query_terms:
                if term in text_lower:
                    score += 1

            if score > 0:
                results.append({"doc": doc, "score": score})

        # Sort by score
        results.sort(key=lambda x: x["score"], reverse=True)

        return [r["doc"] for r in results[:top_k]]

    def clear(self):
        self.index = []


vector_store = SimpleVectorStore()
