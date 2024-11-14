import ast
import logging
from typing import Any, Dict, List, Optional

import kuzu
from haystack import Document, default_from_dict, default_to_dict
from haystack.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.document_stores.types import DuplicatePolicy

logger = logging.getLogger(__name__)


class KuzuDocumentStore:
    def __init__(self, db_path: str):
        """
        Initializes the Kuzu document store.

        Args:
            db_path: Path to the Kuzu database
        """

        self.db = kuzu.Database(db_path)
        self.connection = kuzu.Connection(self.db)

        # Create document table if it doesn't exist
        self.connection.execute(
            """
            CREATE NODE TABLE IF NOT EXISTS documents(
                id STRING,
                content STRING,
                meta STRING,
                PRIMARY KEY (id)
            )
        """
        )

    def count_documents(self) -> int:
        result = self.connection.execute("MATCH (d:documents) RETURN count(d) as count")
        return result.get_next()[0]
    
    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        documents = []
        if not filters:
            # Execute the query to retrieve all documents
            result = self.connection.execute("MATCH (d:documents) RETURN d.id, d.content, d.meta")
            
            while result.has_next():
                row = result.get_next()
                documents.append(
                    Document(id=row[0], content=row[1], meta=ast.literal_eval(row[2]))  # Adjust indices if needed
                )
        # Add handling for filter-based querying if applicable
        return documents

    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE) -> int:
        document_written = 0
        for doc in documents:
            # Check for document existence
            result = self.connection.execute("MATCH (d:documents) WHERE d.id = $id RETURN d", {"id": doc.id})
            
            if result.has_next():
                # Document already exists
                if policy == "fail":
                    raise DuplicateDocumentError(f"Document with id {doc.id} already exists.")
                elif policy == "skip":
                    continue
                elif policy == "overwrite":
                    # Delete the existing document with the same id
                    self.connection.execute("MATCH (d:documents) WHERE d.id = $id DELETE d", {"id": doc.id})
            
            # Insert the document
            self.connection.execute(
                """
                CREATE (d:documents {
                    id: $id,
                    content: $content,
                    meta: $meta
                })
                """,
                {
                    "id": doc.id,
                    "content": doc.content,
                    "meta": str(doc.meta),
                }
            )
            document_written += 1

        return document_written

    def delete_documents(self, document_ids: List[str]) -> None:
        for doc_id in document_ids:
            result = self.connection.execute("MATCH (d:documents) WHERE d.id = $id RETURN d.id", {"id": doc_id})
            if result.get_next() is None:
                msg = f"ID '{doc_id}' not found, cannot delete it."
                raise MissingDocumentError(msg)

            self.connection.execute("MATCH (d:documents) WHERE d.id = $id DELETE d", {"id": doc_id})

    def to_dict(self) -> Dict[str, Any]:
        """Serializes this store to a dictionary."""
        return default_to_dict(self, db_path=self.db.path)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KuzuDocumentStore":
        """Deserializes the store from a dictionary."""
        return default_from_dict(cls, data)
