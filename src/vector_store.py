import chromadb
from chromadb import Collection

from config import CHROMA_COLLECTION_NAME, CHROMA_PERSIST_DIR

_client: chromadb.ClientAPI | None = None
_collection: Collection | None = None


def _get_client() -> chromadb.ClientAPI:
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
    return _client


def get_collection() -> Collection:
    global _collection
    if _collection is None:
        _collection = _get_client().get_or_create_collection(name=CHROMA_COLLECTION_NAME)
    return _collection


def upsert_log(
    event_id: str,
    embedding: list[float],
    metadata: dict,
    document_text: str,
) -> None:
    get_collection().upsert(
        ids=[event_id],
        embeddings=[embedding],
        metadatas=[metadata],
        documents=[document_text],
    )


def count() -> int:
    return get_collection().count()


def query_similar(query_embedding: list[float], k: int = 5) -> dict:
    return get_collection().query(
        query_embeddings=[query_embedding],
        n_results=k,
    )
