import ast
import logging
import json
from typing import Any, Dict, List, Optional

import kuzu
from haystack import Document, default_from_dict, default_to_dict
from haystack.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.document_stores.types import DuplicatePolicy
from haystack.core.errors import DeserializationError

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

        # Define document schema with separate fields for different `meta` data types
        self.connection.execute(
            """
            CREATE NODE TABLE IF NOT EXISTS documents(
                id STRING,
                content STRING,
                meta_STRING MAP(STRING, STRING),
                meta_INT MAP(STRING, INT64),
                meta_FLOAT MAP(STRING, FLOAT),
                embedding FLOAT[],
                PRIMARY KEY (id)
            )
            """
        )
        logger.info("Initialized KuzuDocumentStore with database at %s", db_path)

    def count_documents(self) -> int:
        """
        Counts the number of documents in the store.
        """
        result = self.connection.execute("MATCH (d:documents) RETURN count(d) as count")
        return result.get_next()[0]

    def _categorize_meta(self, meta: Dict[str, Any]) -> Dict[str, Dict[str, List[Any]]]:
        """
        Categorizes meta into `meta_STRING`, `meta_INT`, and `meta_FLOAT` fields,
        each formatted as `{"key": [...], "value": [...]}` dictionaries.
        """
        meta_dict = {
            "meta_STRING": {"key": [], "value": []},
            "meta_INT": {"key": [], "value": []},
            "meta_FLOAT": {"key": [], "value": []}
        }

        for key, value in meta.items():
            if isinstance(value, str):
                meta_dict["meta_STRING"]["key"].append(key)
                meta_dict["meta_STRING"]["value"].append(value)
            elif isinstance(value, int):
                meta_dict["meta_INT"]["key"].append(key)
                meta_dict["meta_INT"]["value"].append(value)
            elif isinstance(value, float):
                meta_dict["meta_FLOAT"]["key"].append(key)
                meta_dict["meta_FLOAT"]["value"].append(value)
            else:
                logger.warning(f"Unsupported meta type for key {key}: {type(value).__name__}")

        return meta_dict


    def write_documents(self, documents: List[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE) -> int:
        """
        Writes documents to the store, handling metadata by type.
        """
        document_written = 0
        for doc in documents:
            # Check if the document already exists
            result = self.connection.execute("MATCH (d:documents) WHERE d.id = $id RETURN d", {"id": doc.id})

            # Convert policy to DuplicatePolicy if it is passed as a string
            if isinstance(policy, str):
                policy = DuplicatePolicy(policy)
            
            if result.has_next():
                if policy == DuplicatePolicy.FAIL:
                    raise DuplicateDocumentError(f"Document with id {doc.id} already exists.")
                elif policy == DuplicatePolicy.SKIP:
                    continue
                elif policy == DuplicatePolicy.OVERWRITE:
                    # Delete the existing document with the same id
                    self.connection.execute("MATCH (d:documents) WHERE d.id = $id DELETE d", {"id": doc.id})

            # Categorize meta data by type
            categorized_meta = self._categorize_meta(doc.meta or {})

            # Define query to create a document node with type-specific metadata fields
            query = """
            CREATE (d:documents {
                id: $id,
                content: $content,
                meta_STRING: $meta_STRING,
                meta_INT: $meta_INT,
                meta_FLOAT: $meta_FLOAT
            })
            """
            params = {
                "id": doc.id,
                "content": doc.content,
                "meta_STRING": categorized_meta["meta_STRING"],
                "meta_INT": categorized_meta["meta_INT"],
                "meta_FLOAT": categorized_meta["meta_FLOAT"]
            }
            self.connection.execute(query, params)
            document_written += 1

        return document_written


    def delete_documents(self, document_ids: List[str]) -> None:
        """
        Deletes documents from the store.
        """
        for doc_id in document_ids:
            result = self.connection.execute("MATCH (d:documents) WHERE d.id = $id RETURN d.id", {"id": doc_id})
            if result.get_next() is None:
                raise MissingDocumentError(f"ID '{doc_id}' not found, cannot delete it.")

            self.connection.execute("MATCH (d:documents) WHERE d.id = $id DELETE d", {"id": doc_id})

    def _build_filter_query(self, filters: Dict[str, Any]) -> str:
        """
        Builds a WHERE clause for filtering documents.
        """
        if not filters:
            return ""

        # Check if `filters` is a single condition (not nested)
        if "field" in filters and "operator" in filters and "value" in filters:
            # Handle as a single condition
            return self._build_single_condition(filters)
        
        # If not a single condition, treat as nested conditions
        operator = filters.get("operator", "AND").upper()
        if operator not in ["AND", "OR", "NOT"]:
            raise ValueError("Operator must be 'AND', 'OR', or 'NOT'.")

        conditions = []
        for condition in filters.get("conditions", []):
            if "conditions" in condition:
                # Recursively build nested conditions
                nested_query = self._build_filter_query(condition)
                conditions.append(f"NOT ({nested_query})" if operator == "NOT" else f"({nested_query})")
            else:
                # Build individual condition
                conditions.append(self._build_single_condition(condition))

        # Join conditions with the specified operator
        joined_conditions = f" {operator} ".join(conditions)
        return f"({joined_conditions})" if len(conditions) > 1 else joined_conditions

    def _build_single_condition(self, condition: Dict[str, Any]) -> str:
        """
        Builds a single condition for the WHERE clause.
        """
        field = condition["field"]
        if not field.startswith("meta."):
            raise ValueError(f"Unsupported field format: {field}")
        key = field.split("meta.", 1)[1]
        value = condition["value"]
        op = condition["operator"]

        # Determine the correct field access based on type
        if isinstance(value, str):
            field_access = f"map_extract(d.meta_STRING, '{key}')[1]"
        elif isinstance(value, int):
            field_access = f"map_extract(d.meta_INT, '{key}')[1]"
        elif isinstance(value, float):
            field_access = f"map_extract(d.meta_FLOAT, '{key}')[1]"
        else:
            raise ValueError(f"Unsupported filter value type: {type(value).__name__}")

        # Safely format the value
        formatted_value = self._format_value(value)

        # Build the condition based on the operator
        if op == "==":
            return f"{field_access} = {formatted_value}"
        elif op == "!=":
            return f"{field_access} <> {formatted_value}"
        elif op == ">=":
            return f"{field_access} >= {formatted_value}"
        elif op == "<=":
            return f"{field_access} <= {formatted_value}"
        elif op == ">":
            return f"{field_access} > {formatted_value}"
        elif op == "<":
            return f"{field_access} < {formatted_value}"
        elif op == "in":
            if not isinstance(value, list):
                raise ValueError("Operator 'in' requires a list of values.")
            value_list = ", ".join([self._format_value(v) for v in value])
            return f"{field_access} IN [{value_list}]"
        elif op == "not in":
            if not isinstance(value, list):
                raise ValueError("Operator 'not in' requires a list of values.")
            value_list = ", ".join([self._format_value(v) for v in value])
            return f"{field_access} NOT IN [{value_list}]"
        else:
            raise ValueError(f"Unsupported operator: {op}")

    def _format_value(self, value):
        """
        Formats value for Cypher syntax.
        """
        if isinstance(value, str):
            return f"'{value}'"
        elif value is None:
            return "null"
        return value

    def filter_documents(self, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        """
        Returns the documents that match the filters provided.

        Filters are defined as nested dictionaries that can be of two types:
        - Comparison
        - Logic

        Comparison dictionaries must contain the keys:

        - `field`
        - `operator`
        - `value`

        Logic dictionaries must contain the keys:

        - `operator`
        - `conditions`

        The `conditions` key must be a list of dictionaries, either of type Comparison or Logic.

        The `operator` value in Comparison dictionaries must be one of:

        - `==`
        - `!=`
        - `>`
        - `>=`
        - `<`
        - `<=`
        - `in`
        - `not in`

        The `operator` values in Logic dictionaries must be one of:

        - `NOT`
        - `OR`
        - `AND`


        A simple filter:
        ```python
        filters = {"field": "meta.type", "operator": "==", "value": "article"}
        ```

        A more complex filter:
        ```python
        filters = {
            "operator": "AND",
            "conditions": [
                {"field": "meta.type", "operator": "==", "value": "article"},
                {"field": "meta.date", "operator": ">=", "value": 1420066800},
                {"field": "meta.date", "operator": "<", "value": 1609455600},
                {"field": "meta.rating", "operator": ">=", "value": 3},
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "meta.genre", "operator": "in", "value": ["economy", "politics"]},
                        {"field": "meta.publisher", "operator": "==", "value": "nytimes"},
                    ],
                },
            ],
        }

        :param filters: the filters to apply to the document list.
        :return: a list of Documents that match the given filters.
        """
        documents = []
        query = "MATCH (d:documents) "

        if filters:
            where_clause = self._build_filter_query(filters)
            query += f"WHERE {where_clause} "

        query += "RETURN d.id, d.content, d.meta_STRING, d.meta_INT, d.meta_FLOAT"

        result = self.connection.execute(query)

        while result.has_next():
            row = result.get_next()
            doc_id, content, meta_string, meta_int, meta_float = row

            # Initialize an empty meta dictionary
            meta = {}

           # Directly update meta with the existing dictionaries if they are not empty
            if meta_string:
                meta.update(meta_string)
            if meta_int:
                meta.update(meta_int)
            if meta_float:
                meta.update(meta_float)

            # Append the document with parsed metadata to the list
            documents.append(Document(id=doc_id, content=content, meta=meta))

        return documents


    def to_dict(self) -> Dict[str, Any]:
        """Serializes this store to a dictionary."""
        return {
            "type": "KuzuDocumentStore",
            "db_path": self.db.database_path
        }


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KuzuDocumentStore":
        """Deserializes the store from a dictionary.

        :param data: The dictionary containing the serialized data.
        :returns: The deserialized KuzuDocumentStore object.
        :raises DeserializationError: If the `type` field in `data` is missing or it doesn't match the type of `cls`.
        """
        # Check for the 'type' field in data to ensure it matches the expected class
        if data.get("type") != "KuzuDocumentStore":
            raise DeserializationError("Missing or incorrect 'type' in serialization data")
        
        db_path = data.get("db_path")

        if not db_path:
            raise DeserializationError("Missing 'db_path' in 'init_parameters'")

        # Create and return a new instance of KuzuDocumentStore with the extracted parameters
        return cls(db_path=db_path)
