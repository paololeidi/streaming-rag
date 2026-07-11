from sentence_transformers import SentenceTransformer

from config import EMBEDDING_MODEL_NAME


class HuggingFaceEmbedder:
    def __init__(self, model_name: str = EMBEDDING_MODEL_NAME):
        self._model_name = model_name
        self._model: SentenceTransformer | None = None

    def _get_model(self) -> SentenceTransformer:
        if self._model is None:
            self._model = SentenceTransformer(self._model_name)
        return self._model

    def embed(self, text: str) -> list[float]:
        return self._get_model().encode(text).tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return self._get_model().encode(texts).tolist()
