from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator   
class Contact(BaseModel):
    name: str
    role: str
    email: str

class Insured(BaseModel):
    named_insured: str
    trading_name: str
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
    pool: str

class Policy(BaseModel):
    submission_ref: str
    class_: str = Field(alias="class")
    inception_date: date
    expiry_date: date
    policy_period_months: int
    underwriter: str
    underwriter_division: Optional[str] = None
    policy_form: str
    perils: List[str]
    excess_standard_gbp: float
    excess_flood_gbp: Optional[float] = None
    terrorism: Optional[Terrorism] = None

    @field_validator("policy_period_months")
    def validate_period(cls, v):
        if v not in [12, 24, 36]:
            raise ValueError("Must be 12, 24 or 36")
        return v

class BusinessInterruption(BaseModel):
    included: bool
    indemnity_months: int
    gross_profit_gbp: float
    basis: str

class MachineryBreakdown(BaseModel):
    included: bool
    sublimit_gbp: Optional[float] = None
    location_ids: Optional[List[str]] = None

class Coverage(BaseModel):
    total_building_si_gbp: float
    total_contents_si_gbp: float
    total_tiv_gbp: float
    business_interruption: Optional[BusinessInterruption] = None
    machinery_breakdown: Optional[MachineryBreakdown] = None

    @field_validator("total_tiv_gbp")
    def validate_tiv(cls, v, info):
        data = info.data
        if "total_building_si_gbp" in data and "total_contents_si_gbp" in data:
            expected = data["total_building_si_gbp"] + data["total_contents_si_gbp"]
            if abs(v - expected) > 0.01:
                raise ValueError(f"total_tiv_gbp must equal {expected}")
        return v

class Location(BaseModel):
    location_id: str
    address: str
    occupancy: str
    construction: str
    year_built: Optional[int] = None
    year_refurbished: Optional[int] = None
    sqft: int
    building_si_gbp: float
    contents_si_gbp: float
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
    location_id: str
    peril: str
    gross_incurred_gbp: float
    net_paid_gbp: float
    status: str  # open, closed, reserved

class DocumentEnclosed(BaseModel):
    title: str
    appendix: str
    status: str  # received, pending

class UnderwritingSubmission(BaseModel):
    correlation_id: str
    schema_version: str = "v3"
    timestamp_extracted: datetime
    source_file_hash: str
    insured: Insured
    policy: Policy
    coverage: Coverage
    locations: List[Location]
    risk_features: Optional[RiskFeatures] = None
    loss_history: List[LossHistory] = []
    documents_enclosed: Optional[List[DocumentEnclosed]] = None

