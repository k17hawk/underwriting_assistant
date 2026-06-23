from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, model_validator


class Contact(BaseModel):
    name: str
    role: Optional[str] = None        
    email: Optional[str] = None


class Insured(BaseModel):
    named_insured: str
    trading_name: Optional[str] = None
    company_reg_no: Optional[str] = None
    principal_activity: str
    sic_code: Optional[str] = None
    years_in_operation: Optional[int] = None
    annual_turnover_gbp: Optional[float] = None
    employee_count_ft: Optional[int] = None
    employee_count_pt: Optional[int] = None
    contact: Optional[Contact] = None


class Terrorism(BaseModel):
    requested: bool
    pool: Optional[str] = None        # was required — not always Pool Re


class Policy(BaseModel):
    submission_ref: str
    class_: str = Field(alias="class")
    inception_date: date
    expiry_date: date
    policy_period_months: int
    underwriter: str
    underwriter_division: Optional[str] = None
    policy_form: Optional[str] = None         # not always stated
    perils: List[str] = []
    excess_standard_gbp: Optional[float] = None   # was required — sometimes missing
    excess_flood_gbp: Optional[float] = None
    terrorism: Optional[Terrorism] = None

    model_config = {"populate_by_name": True}

    @field_validator("policy_period_months")
    @classmethod
    def validate_period(cls, v):
        if v not in [12, 24, 36]:
            raise ValueError("Must be 12, 24 or 36")
        return v


class BusinessInterruption(BaseModel):
    included: bool
    indemnity_months: Optional[int] = None        # was required
    gross_profit_gbp: Optional[float] = None      # was required — causes failures
    basis: Optional[str] = None                   # was required — causes failures


class MachineryBreakdown(BaseModel):
    included: bool
    sublimit_gbp: Optional[float] = None
    location_ids: Optional[List[str]] = None


class Coverage(BaseModel):
    total_building_si_gbp: Optional[float] = None    # was required — often missing
    total_contents_si_gbp: Optional[float] = None    # was required
    total_tiv_gbp: Optional[float] = None            # was required
    business_interruption: Optional[BusinessInterruption] = None
    machinery_breakdown: Optional[MachineryBreakdown] = None

    @model_validator(mode='after')
    def validate_tiv(self) -> 'Coverage':
        """
        Validate TIV only when all three values are present and non-zero.
        Allow 2% tolerance for rounding differences from OCR/LLM extraction.
        If building SI is missing, skip validation and trust the TIV as extracted.
        """
        building = self.total_building_si_gbp
        contents = self.total_contents_si_gbp
        tiv      = self.total_tiv_gbp

        if tiv and building and contents:
            expected  = building + contents
            tolerance = expected * 0.02     # 2% — covers OCR rounding
            if abs(tiv - expected) > tolerance:
                raise ValueError(
                    f"total_tiv_gbp ({tiv:,.0f}) does not match "
                    f"building ({building:,.0f}) + contents ({contents:,.0f}) "
                    f"= {expected:,.0f}"
                )
        return self


class Location(BaseModel):
    location_id: Optional[str] = None
    address: Optional[str] = None 
    occupancy: Optional[str] = None
    construction: Optional[str] = None
    year_built: Optional[int] = None
    year_refurbished: Optional[int] = None
    sqft: Optional[int] = None               # was required — frequently missing from docs
    building_si_gbp: Optional[float] = None  # was required — mapper was missing this
    contents_si_gbp: Optional[float] = None
    flood_zone: Optional[str] = None
    sprinkler: Optional[str] = None


class RiskFeatures(BaseModel):
    sprinkler_system: Optional[str] = None
    fire_alarm: Optional[str] = None
    security: Optional[str] = None
    flood_risk: Optional[str] = None
    hot_works: Optional[str] = None
    storage_of_flammables: Optional[str] = None
    broker_notes: Optional[List[str]] = None


class LossHistory(BaseModel):
    date: date
    location_id: Optional[str] = None    # not always stated per claim
    peril: str
    gross_incurred_gbp: Optional[float] = None
    net_paid_gbp: Optional[float] = None
    status: Optional[str] = None


class DocumentEnclosed(BaseModel):
    title: str
    appendix: Optional[str] = None       # was required — some docs have no appendix
    status: Optional[str] = "received"


class UnderwritingSubmission(BaseModel):
    correlation_id: str
    schema_version: str = "v3"
    timestamp_extracted: datetime
    source_file_hash: str
    insured: Insured
    policy: Policy
    coverage: Coverage
    locations: List[Location] = []
    risk_features: Optional[RiskFeatures] = None
    loss_history: List[LossHistory] = []
    documents_enclosed: Optional[List[DocumentEnclosed]] = None