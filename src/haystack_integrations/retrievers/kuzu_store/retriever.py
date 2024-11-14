import ast
from typing import Any, Dict, Optional

from haystack import component

from haystack_integrations.document_stores.kuzu_store import KuzuDocumentStore


@component
class KuzuRetriever:
    """
    A component for retrieving documents from an KuzuDocumentStore.
    """

    def __init__(self, document_store: KuzuDocumentStore, filters: Optional[Dict[str, Any]] = None, top_k: int = 10):
        """
        Create an KuzuRetriever component.

        :param document_store: A Document Store object used to retrieve documents
        :param filters: A dictionary with filters to narrow down the search space
        :param top_k: The maximum number of documents to retrieve
        :raises ValueError: If the specified top_k is not > 0.
        """
        if top_k <= 0:
            msg = f"top_k must be > 0, but got {top_k}"
            raise ValueError(msg)

        self.filters = filters
        self.top_k = top_k
        self.document_store = document_store

    def run(self, query: str):
        """
        Run the Retriever on the given query.

        :param query: The search query string
        :return: Dictionary containing retrieved documents
        """

        # Execute search query using Kuzu's capabilities
        results = self.document_store.connection.execute(
            """
            MATCH (d:documents)
            WHERE d.content CONTAINS $query
            RETURN d.id, d.content, d.meta
            LIMIT $limit
        """,
            {"query": query, "limit": self.top_k},
        )

        retrieved_docs = []
        while row := results.get_next():
            doc = {"id": row["d.id"], "content": row["d.content"], "meta": ast.literal_eval(row["d.meta"])}
            retrieved_docs.append(doc)

        return {"documents": retrieved_docs[: self.top_k]}
