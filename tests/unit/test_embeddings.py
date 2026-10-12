"""Unit tests for HuggingFaceEmbedder."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np

import embeddings


def test_embedder_lazy_loading(monkeypatch):
    """Test SentenceTransformer model is initialized lazily and cached."""
    mock_st_class = MagicMock()
    mock_model = MagicMock()
    mock_st_class.return_value = mock_model
    monkeypatch.setattr(embeddings, "SentenceTransformer", mock_st_class)

    embedder = embeddings.HuggingFaceEmbedder(model_name="test-model")
    assert embedder._model is None

    # First access
    m1 = embedder._get_model()
    mock_st_class.assert_called_once_with("test-model")
    assert m1 is mock_model

    # Second access (should use cached model)
    m2 = embedder._get_model()
    assert m2 is mock_model
    mock_st_class.assert_called_once()


def test_embed_single_text(monkeypatch):
    """Test embed converts single text into list of float embeddings."""
    mock_model = MagicMock()
    mock_model.encode.return_value = np.array([0.1, 0.2, 0.3])
    monkeypatch.setattr(embeddings, "SentenceTransformer", lambda name: mock_model)

    embedder = embeddings.HuggingFaceEmbedder()
    res = embedder.embed("sample query")

    assert res == [0.1, 0.2, 0.3]
    mock_model.encode.assert_called_once_with("sample query")


def test_embed_batch_texts(monkeypatch):
    """Test embed_batch converts multiple texts into list of float lists."""
    mock_model = MagicMock()
    mock_model.encode.return_value = np.array([[0.1, 0.2], [0.3, 0.4]])
    monkeypatch.setattr(embeddings, "SentenceTransformer", lambda name: mock_model)

    embedder = embeddings.HuggingFaceEmbedder()
    res = embedder.embed_batch(["text1", "text2"])

    assert res == [[0.1, 0.2], [0.3, 0.4]]
    mock_model.encode.assert_called_once_with(["text1", "text2"])
