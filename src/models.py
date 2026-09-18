from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

from enum import Enum

class JobStatus(str, Enum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class SrtGenerationRequest(BaseModel):
    job_id: str = Field(alias="jobId")
    object_key: str = Field(alias="objectKey")
    source_language: Optional[str] = Field(default=None, alias="sourceLanguage")
    target_language: Optional[str] = Field(default=None, alias="targetLanguage")
    requested_at: datetime = Field(alias="requestedAt")

class SrtGenerationResult(BaseModel):
    job_id: str = Field(alias="jobId")
    status: JobStatus
    srt_object_key: str | None = Field(default=None, alias="srtObjectKey")
    detected_language: str | None = Field(default=None, alias="detectedLanguage")
    error: str | None = None

    class Config:
        populate_by_name = True