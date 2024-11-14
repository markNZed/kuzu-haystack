# SPDX-FileCopyrightText: 2023-present John Doe <jd@example.com>
#
# SPDX-License-Identifier: Apache-2.0
import pytest
from haystack.testing.document_store import DocumentStoreBaseTests

from haystack_integrations.document_stores.kuzu_store import KuzuDocumentStore


@pytest.mark.skip("This is an example Document Store")
class TestDocumentStore(DocumentStoreBaseTests):
    """
    Common test cases will be provided by `DocumentStoreBaseTests` but
    you can add more to this class.
    """

    @pytest.fixture
    def docstore(self) -> KuzuDocumentStore:
        """
        This is the most basic requirement for the child class: provide
        an instance of this document store so the base class can use it.
        """
        return KuzuDocumentStore()
