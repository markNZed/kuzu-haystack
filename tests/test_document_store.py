# SPDX-FileCopyrightText: 2023-present John Doe <jd@example.com>
#
# SPDX-License-Identifier: Apache-2.0

import os
import pytest
from haystack.testing.document_store import DocumentStoreBaseTests
from haystack import Document
from haystack.document_stores.errors import DuplicateDocumentError
from haystack_integrations.document_stores.kuzu_store import KuzuDocumentStore

class TestKuzuDocumentStore(DocumentStoreBaseTests):
    """
    Test cases for KuzuDocumentStore implementation
    """

    @pytest.fixture
    def docstore(self, tmp_path) -> KuzuDocumentStore:
        """
        Creates a fresh KuzuDocumentStore instance for each test
        """
        db_path = str(tmp_path / "kuzu_test.db")
        return KuzuDocumentStore(db_path=db_path)

    def test_write_and_read_documents(self, docstore):
        docs = [
            Document(content="test1", meta={"key1": "value1"}),
            Document(content="test2", meta={"key2": "value2"})
        ]
        docstore.write_documents(docs)
        assert docstore.count_documents() == 2
        
        retrieved = docstore.filter_documents()
        assert len(retrieved) == 2
        assert retrieved[0].content in ["test1", "test2"]

    def test_duplicate_policy(self, docstore):
        doc = Document(content="test", id="1")
        docstore.write_documents([doc])
        
        # Test FAIL policy
        with pytest.raises(DuplicateDocumentError):
            docstore.write_documents([doc], policy="fail")

        # Test SKIP policy
        assert docstore.write_documents([doc], policy="skip") == 0

        # Test OVERWRITE policy
        new_doc = Document(content="updated", id="1")
        assert docstore.write_documents([new_doc], policy="overwrite") == 1
        retrieved = docstore.filter_documents()
        assert retrieved[0].content == "updated"

    def test_complex_filters(self, docstore):
        docs = [
            Document(content="doc1", meta={"type": "article", "rating": 4}),
            Document(content="doc2", meta={"type": "blog", "rating": 3}),
            Document(content="doc3", meta={"type": "article", "rating": 5})
        ]
        docstore.write_documents(docs)

        filters = {
            "operator": "AND",
            "conditions": [
                {"field": "meta.type", "operator": "==", "value": "article"},
                {"field": "meta.rating", "operator": ">=", "value": 4}
            ]
        }
        
        results = docstore.filter_documents(filters)
        assert len(results) == 2
        assert all(d.meta["type"] == "article" and d.meta["rating"] >= 4 for d in results)

    def test_delete_documents(self, docstore):
        docs = [
            Document(content="test1", id="1"),
            Document(content="test2", id="2")
        ]
        docstore.write_documents(docs)
        assert docstore.count_documents() == 2

        docstore.delete_documents(["1"])
        assert docstore.count_documents() == 1
        
        retrieved = docstore.filter_documents()
        assert retrieved[0].id == "2"

    def test_serialization(self, docstore, tmp_path):
        docs = [Document(content="test")]
        docstore.write_documents(docs)
        
        serialized = docstore.to_dict()
        assert "db_path" in serialized
        
        new_store = KuzuDocumentStore.from_dict(serialized)
        assert new_store.count_documents() == 1
