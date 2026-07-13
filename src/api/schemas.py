"""Pydantic request/response models for the query API."""

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    prompt: str = Field(
        min_length=1,
        description="Natural-language question about system health or log activity.",
        examples=["What caused the payment service to crash?"],
    )
    k: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of log entries to retrieve from the vector store.",
    )


class LogSource(BaseModel):
    service_name: str
    log_level: str
    timestamp: str
    excerpt: str = Field(description="Relevant portion of the log document.")


class QueryResponse(BaseModel):
    answer: str = Field(description="Agent's final answer to the user's prompt.")
    sources: list[LogSource] = Field(
        default_factory=list,
        description="Log entries cited by the agent (populated from vector_search results).",
    )
