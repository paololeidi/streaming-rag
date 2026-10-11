"""Unit tests for vector_store module."""

from __future__ import annotations

from unittest.mock import MagicMock

import vector_store


def test_upsert_log(monkeypatch):
    """Test upsert_log proxies parameters to collection upsert."""
    mock_coll = MagicMock()
    monkeypatch.setattr(vector_store, "get_collection", lambda: mock_coll)

    vector_store.upsert_log(
        event_id="evt-1",
        embedding=[0.1, 0.2],
        metadata={"service": "auth"},
        document_text="sample text",
    )

    mock_coll.upsert.assert_called_once_with(
        ids=["evt-1"],
        embeddings=[[0.1, 0.2]],
        metadatas=[{"service": "auth"}],
        documents=["sample text"],
    )


def test_count(monkeypatch):
    """Test count returns collection record count."""
    mock_coll = MagicMock()
    mock_coll.count.return_value = 42
    monkeypatch.setattr(vector_store, "get_collection", lambda: mock_coll)

    assert vector_store.count() == 42


def test_query_similar(monkeypatch):
    """Test query_similar proxies embedding and k to collection query."""
    mock_coll = MagicMock()
    mock_coll.query.return_value = {"documents": [["doc1"]], "metadatas": [[{}]]}
    monkeypatch.setattr(vector_store, "get_collection", lambda: mock_coll)

    res = vector_store.query_similar(query_embedding=[0.3, 0.4], k=7)
    mock_coll.query.assert_called_once_with(
        query_embeddings=[[0.3, 0.4]],
        n_results=7,
    )
    assert res == {"documents": [["doc1"]], "metadatas": [[{}]]}
