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


def test_get_client_persistent(monkeypatch):
    """Test _get_client creates PersistentClient when CHROMA_HOST is empty."""
    vector_store._client = None
    monkeypatch.setattr(vector_store, "CHROMA_HOST", "")
    mock_pc = MagicMock()
    monkeypatch.setattr(vector_store.chromadb, "PersistentClient", lambda path: mock_pc)

    client1 = vector_store._get_client()
    client2 = vector_store._get_client()

    assert client1 is mock_pc
    assert client2 is mock_pc


def test_get_client_http(monkeypatch):
    """Test _get_client creates HttpClient when CHROMA_HOST is set."""
    vector_store._client = None
    monkeypatch.setattr(vector_store, "CHROMA_HOST", "chroma-svc")
    monkeypatch.setattr(vector_store, "CHROMA_PORT", 8000)
    mock_hc = MagicMock()
    monkeypatch.setattr(vector_store.chromadb, "HttpClient", lambda host, port: mock_hc)

    client = vector_store._get_client()
    assert client is mock_hc


def test_get_collection_caching(monkeypatch):
    """Test get_collection creates collection on client and caches it."""
    vector_store._collection = None
    mock_client = MagicMock()
    mock_coll = MagicMock()
    mock_client.get_or_create_collection.return_value = mock_coll
    monkeypatch.setattr(vector_store, "_get_client", lambda: mock_client)

    coll1 = vector_store.get_collection()
    coll2 = vector_store.get_collection()

    assert coll1 is mock_coll
    assert coll2 is mock_coll
    mock_client.get_or_create_collection.assert_called_once()
