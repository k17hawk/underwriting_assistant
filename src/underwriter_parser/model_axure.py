from datetime import datetime
from typing import Optional, List, Any, Dict, Union
from pydantic import BaseModel, Field, field_validator

class FieldValue(BaseModel):
    value: Any
    page_ref: str  # Now required and validated
    confidence: float = Field(ge=0.0, le=1.0)
    
    @field_validator('page_ref')
    def validate_page_ref(cls, v):
        if not v or not v.startswith('p'):
            raise ValueError(f"page_ref must start with 'p', got: {v}")
        try:
            page_num = int(v[1:])
            if page_num < 1:
                raise ValueError(f"page number must be >= 1, got: {page_num}")
        except ValueError:
            raise ValueError(f"page_ref must be in format 'p<number>', got: {v}")
        return v

class MetadataField(BaseModel):
    value: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class Metadata(BaseModel):
    document_type: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    fields: Dict[str, MetadataField]

class InsuredField(BaseModel):
    value: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class LocationField(BaseModel):
    value: Any
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class Location(BaseModel):
    loc_id: str
    loc_number: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    fields: Dict[str, LocationField]

class CoverageField(BaseModel):
    value: Any
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class RiskField(BaseModel):
    value: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class Note(BaseModel):
    note_id: str
    type: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    text: str

class ClaimField(BaseModel):
    value: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class Claim(BaseModel):
    claim_id: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    fields: Dict[str, ClaimField]

class SummaryField(BaseModel):
    value: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class LossSummary(BaseModel):
    loss_ratio_5yr_pct_of_tiv: SummaryField
    claims_last_12_months: SummaryField
    open_reserves: SummaryField

class Document(BaseModel):
    doc_id: str
    description: str
    appendix: Optional[str] = None
    status: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)

class Section(BaseModel):
    section_id: str
    section_title: str
    section_number: int
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    fields: Optional[Dict[str, Any]] = None
    locations: Optional[List[Location]] = None
    summary: Optional[LossSummary] = None
    claims: Optional[List[Claim]] = None
    documents: Optional[List[Document]] = None
    notes: Optional[List[Note]] = None

class DocumentSection(BaseModel):
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    metadata: Metadata
    sections: List[Section]

class RagChunk(BaseModel):
    chunk_id: str
    chunk_type: str
    section_ref: str
    page_ref: str
    confidence: float = Field(ge=0.0, le=1.0)
    text: str
    location_ref: Optional[str] = None

class AzureDocumentOutput(BaseModel):
    api_version: str = "2024-02-29-preview"
    model_id: str = "prebuilt-document"
    document_source: str
    parsed_at: datetime
    page_count: int
    document: DocumentSection
    rag_chunks: List[RagChunk] = []
    
    @field_validator('page_count')
    def validate_page_count(cls, v, values):
        # Ensure page_count matches actual document
        if 'document' in values:
            doc = values['document']
            max_page = 0
            if doc.metadata.page_ref:
                try:
                    max_page = max(max_page, int(doc.metadata.page_ref[1:]))
                except:
                    pass
            for section in doc.sections:
                if section.page_ref:
                    try:
                        max_page = max(max_page, int(section.page_ref[1:]))
                    except:
                        pass
            if max_page > 0 and max_page != v:
                # Don't raise error, just log warning
                print(f"⚠️ Page count mismatch: {v} vs actual {max_page}")
        return v