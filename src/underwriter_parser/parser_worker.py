import os
import json
import time
import signal
import requests
from pathlib import Path
from typing import Optional, Dict, Any, Union
from datetime import datetime
from confluent_kafka import Consumer, Producer
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
import fitz  
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
import re

from src.underwriter_parser.entity.config import config
from src.underwriter_parser.mongodb_storage import MongoDBSubmissionStore
from src.underwriter_parser.models import UnderwritingSubmission
from src.underwriter_parser.streamer import KafkaHandoff


class ParserWorker:
    def __init__(self):
        self.artifact_dir = Path(config.storage.artifact_dir)
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.running = True
        self.db_store = MongoDBSubmissionStore()
        consumer_conf = {
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'group.id': 'parser-worker-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'max.poll.interval.ms': 60000,
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([config.kafka.parser_input_topic])
        producer_conf = {
            'bootstrap.servers': config.kafka.bootstrap_servers,
            'client.id': 'parser-worker-producer'
        }
        self.producer = Producer(producer_conf)
        self.kafka_handoff = KafkaHandoff(
            bootstrap_servers=config.kafka.bootstrap_servers,
            topic=config.kafka.handoff_topic
        )
        self.deepseek_config = config.deepseek
        self.tracer = trace.get_tracer("parser_worker")
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        print("\n🛑 Shutting down worker...")
        self.running = False

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"❌ Output message delivery failed: {err}")
        else:
            print(f"✅ Output message delivered to {msg.topic()} [{msg.partition()}]")

    # ---------- HELPERS ----------
    def _parse_value(self, value_obj: Union[Dict, Any]) -> Any:
        if isinstance(value_obj, dict) and "value" in value_obj:
            return value_obj["value"]
        return value_obj

    def _parse_number(self, val: str) -> Optional[float]:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return float(val)
        cleaned = re.sub(r'[£,\s]', '', str(val))
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str:
            return None
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        if re.match(r'^\d{4}-\d{2}$', date_str):
            return f"{date_str}-01"
        try:
            dt = datetime.strptime(date_str, "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            dt = datetime.strptime(date_str, "%b %Y")
            return dt.strftime("%Y-%m-01")
        except ValueError:
            pass
        if re.match(r'^\d{4}$', date_str):
            return f"{date_str}-01-01"
        return date_str

    # ---------- NEW HELPER ----------
    def _coerce_to_string(self, val: Any) -> Optional[str]:
        """
        Coerce any value to a plain string.
        Handles the case where DeepSeek returns a per-location dict
        (e.g. {"loc_001": "Full wet pipe", "loc_002": "None"})
        instead of a single string, which Pydantic rejects.
        """
        if val is None:
            return None
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            return "; ".join(f"{k}: {v}" for k, v in val.items())
        if isinstance(val, list):
            return "; ".join(str(i) for i in val)
        return str(val)


    # ---------- AZURE-STYLE FLATTENING ----------
    def _flatten_azure_response(self, azure_json: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Azure-like JSON (with sections) to Pydantic dict."""
        flattened = {}
        doc = azure_json.get("document", {})
        if not doc:
            return flattened

        def get_field(section, field_name):
            if not section:
                return None
            fields = section.get("fields", {})
            if field_name in fields:
                return self._parse_value(fields[field_name])
            return None

        # ----- Insured -----
        insured_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-01"), None
        )
        insured_data = {}
        if insured_section:
            fields = insured_section.get("fields", {})
            emp_raw = self._parse_value(fields.get("no_of_employees"))
            employee_count_ft = None
            employee_count_pt = None
            if isinstance(emp_raw, str):
                ft_match = re.search(r'(\d+)\s*full', emp_raw, re.IGNORECASE)
                pt_match = re.search(r'(\d+)\s*part', emp_raw, re.IGNORECASE)
                if ft_match:
                    employee_count_ft = int(ft_match.group(1))
                if pt_match:
                    employee_count_pt = int(pt_match.group(1))
            elif isinstance(emp_raw, int):
                employee_count_ft = emp_raw

            contact_name  = self._parse_value(fields.get("contact_name"))
            contact_role  = (
                self._parse_value(fields.get("contact_title"))
                or self._parse_value(fields.get("contact_role"))
                or self._parse_value(fields.get("contact_position"))
            )
            contact_email = self._parse_value(fields.get("contact_email"))
            contact = {}
            if contact_name:
                contact["name"] = contact_name
            if contact_role:
                contact["role"] = contact_role
            if contact_email:
                contact["email"] = contact_email

            insured_data = {
                "named_insured":      self._parse_value(fields.get("named_insured")),
                "trading_name":       self._parse_value(fields.get("trading_name")),
                "company_reg_no":     self._parse_value(fields.get("company_reg_no")),
                "principal_activity": self._parse_value(fields.get("principal_activity")),
                "sic_code":           None,
                "years_in_operation": self._parse_value(fields.get("years_in_operation")),
                "annual_turnover_gbp":self._parse_number(self._parse_value(fields.get("annual_turnover"))),
                "employee_count_ft":  employee_count_ft,
                "employee_count_pt":  employee_count_pt,
                "contact":            contact if contact else None,
            }
            insured_data = {k: v for k, v in insured_data.items() if v is not None}
        flattened["insured"] = insured_data

        # ----- Policy -----
        metadata = doc.get("metadata", {})
        meta_fields = metadata.get("fields", {})
        policy_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-03"), None
        )
        terror_val = get_field(policy_section, "terrorism")
        terrorism = None
        if terror_val:
            terrorism = {
                "requested": "requested" in terror_val.lower(),
                "pool": "Pool Re" if "Pool Re" in terror_val else None,
            }
            terrorism = {k: v for k, v in terrorism.items() if v is not None}

        # excess: try multiple field name variants
        excess_std = (
            self._parse_number(get_field(policy_section, "excess_each_loss"))
            or self._parse_number(get_field(policy_section, "excess_standard"))
            or self._parse_number(get_field(policy_section, "excess_all_perils"))
            or self._parse_number(get_field(policy_section, "excess_standard_gbp"))
        )

        policy_data = {
            "submission_ref":       self._parse_value(meta_fields.get("submission_ref")),
            "class":                self._parse_value(meta_fields.get("class")),
            "inception_date":       self._parse_date(get_field(policy_section, "policy_period_start")),
            "expiry_date":          self._parse_date(get_field(policy_section, "policy_period_end")),
            "policy_period_months": get_field(policy_section, "policy_duration_months") or 12,
            "underwriter":          self._parse_value(meta_fields.get("underwriter")),
            "underwriter_division": self._parse_value(meta_fields.get("underwriter_division")),
            "policy_form":          get_field(policy_section, "policy_form"),
            "perils":               get_field(policy_section, "perils") or [],
            "excess_standard_gbp":  excess_std,
            "excess_flood_gbp":     self._parse_number(get_field(policy_section, "excess_flood")),
            "terrorism":            terrorism,
        }
        policy_data = {k: v for k, v in policy_data.items() if v is not None}
        flattened["policy"] = policy_data

        # ----- Coverage -----
        bi_val = get_field(policy_section, "business_interruption")
        business_interruption = None
        if bi_val:
            if isinstance(bi_val, dict):
                bi_included = bi_val.get("included", False)
                bi_months   = bi_val.get("indemnity_period_months") or bi_val.get("indemnity_months")
                bi_gp       = self._parse_number(bi_val.get("gross_profit") or bi_val.get("gross_profit_gbp"))
                bi_basis    = bi_val.get("basis", "Gross Profit" if bi_gp else None)
            else:
                bi_str      = str(bi_val)
                bi_included = bi_str.lower().startswith("yes")
                months_m    = re.search(r'(\d+)[- ]month', bi_str, re.IGNORECASE)
                amount_m    = re.search(r'£([\d,]+)', bi_str)
                bi_months   = int(months_m.group(1)) if months_m else None
                bi_gp       = float(amount_m.group(1).replace(',', '')) if amount_m else None
                bi_basis    = "Gross Profit" if bi_gp else None
            business_interruption = {
                "included":          bi_included,
                "indemnity_months":  bi_months,
                "gross_profit_gbp":  bi_gp,
                "basis":             bi_basis,
            }
            business_interruption = {k: v for k, v in business_interruption.items() if v is not None}

        mb_val = get_field(policy_section, "machinery_breakdown")
        machinery_breakdown = None
        if mb_val:
            if isinstance(mb_val, dict):
                mb_included = mb_val.get("included", False)
                mb_sublimit = self._parse_number(mb_val.get("sublimit") or mb_val.get("sublimit_gbp"))
                mb_locs     = mb_val.get("location_ids", [])
            else:
                mb_str      = str(mb_val)
                mb_included = mb_str.lower().startswith("yes")
                amount_m    = re.search(r'£([\d,]+)', mb_str)
                loc_m       = re.search(r'Loc\.?\s*(\d+)', mb_str, re.IGNORECASE)
                mb_sublimit = float(amount_m.group(1).replace(',', '')) if amount_m else None
                mb_locs     = [f"00{loc_m.group(1)}"] if loc_m else []
            machinery_breakdown = {
                "included":     mb_included,
                "sublimit_gbp": mb_sublimit,
                "location_ids": mb_locs,
            }
            machinery_breakdown = {k: v for k, v in machinery_breakdown.items() if v is not None}

        coverage_data = {
            "total_building_si_gbp": self._parse_number(get_field(policy_section, "total_building_si")),
            "total_contents_si_gbp": self._parse_number(get_field(policy_section, "total_contents_si")),
            "total_tiv_gbp":         self._parse_number(get_field(policy_section, "total_tiv")),
            "business_interruption": business_interruption,
            "machinery_breakdown":   machinery_breakdown,
        }
        coverage_data = {k: v for k, v in coverage_data.items() if v is not None}
        flattened["coverage"] = coverage_data

        # ----- Locations -----
        location_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-02"), None
        )
        locations = []
        if location_section:
            for loc in location_section.get("locations", []):
                loc_fields = loc.get("fields", {})
                sqft_raw = self._parse_value(loc_fields.get("sqft"))
                if sqft_raw is None:
                    sqft_raw = self._parse_value(loc_fields.get("area_sqft")) or self._parse_value(loc_fields.get("floor_area_sqft"))
                loc_data = {
                    "location_id":     loc.get("loc_number", ""),
                    "address":         self._parse_value(loc_fields.get("address")),
                    "occupancy":       self._parse_value(loc_fields.get("occupancy")),
                    "construction":    self._parse_value(loc_fields.get("construction")),
                    "year_built":      self._parse_value(loc_fields.get("year_built")),
                    "year_refurbished":self._parse_value(loc_fields.get("year_refurb")),
                    "sqft":            sqft_raw,
                    "building_si_gbp": self._parse_number(self._parse_value(loc_fields.get("building_si"))),
                    "contents_si_gbp": self._parse_number(self._parse_value(loc_fields.get("contents_si"))),
                }
                loc_data = {k: v for k, v in loc_data.items() if v is not None}
                locations.append(loc_data)
        flattened["locations"] = locations

        # ----- Risk Features -----
        risk_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-04"), None
        )
        risk_data = {}
        if risk_section:
            fields = risk_section.get("fields", {})
            broker_notes = [
                note.get("text")
                for note in risk_section.get("notes", [])
                if note.get("type") == "broker_note" and note.get("text")
            ]
            risk_data = {
                "sprinkler_system":      self._coerce_to_string(self._parse_value(fields.get("sprinkler_system"))),
                "fire_alarm":            self._coerce_to_string(self._parse_value(fields.get("fire_alarm"))),
                "security":              self._coerce_to_string(self._parse_value(fields.get("security"))),
                "flood_risk":            self._coerce_to_string(self._parse_value(fields.get("flood_risk"))),
                "hot_works":             self._coerce_to_string(self._parse_value(fields.get("hot_works"))),
                "storage_of_flammables": self._coerce_to_string(self._parse_value(fields.get("storage_of_flammables"))),
                "broker_notes":          broker_notes if broker_notes else None,
            }
            risk_data = {k: v for k, v in risk_data.items() if v is not None}
        flattened["risk_features"] = risk_data if risk_data else None

        # ----- Loss History -----
        loss_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-05"), None
        )
        loss_history = []
        if loss_section:
            for claim in loss_section.get("claims", []):
                cf = claim.get("fields", {})
                claim_data = {
                    "date":              self._parse_date(self._parse_value(cf.get("date"))),
                    "location_id":       self._parse_value(cf.get("location")),
                    "peril":             self._parse_value(cf.get("peril")),
                    "gross_incurred_gbp":self._parse_number(self._parse_value(cf.get("gross_incurred"))),
                    "net_paid_gbp":      self._parse_number(self._parse_value(cf.get("net_paid"))),
                    "status":            self._parse_value(cf.get("status")),
                }
                claim_data = {k: v for k, v in claim_data.items() if v is not None}
                if claim_data:
                    loss_history.append(claim_data)
        flattened["loss_history"] = loss_history

        # ----- Documents -----
        doc_section = next(
            (s for s in doc.get("sections", []) if s.get("section_id") == "SEC-06"), None
        )
        documents = []
        if doc_section:
            for doc_item in doc_section.get("documents", []):
                doc_data = {
                    "title":   doc_item.get("description", ""),
                    "appendix":f"Appendix {doc_item.get('appendix')}" if doc_item.get("appendix") else "",
                    "status":  "received" if "pending" not in doc_item.get("status", "").lower() else "pending",
                }
                documents.append(doc_data)
        flattened["documents_enclosed"] = documents

        return flattened

    def _flatten_flat_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback flattening for flat JSON (without sections)."""
        flattened = {}

        # ----- Insured -----
        insured_details = data.get("insured_details", {}) or {}
        contact_raw = insured_details.get("contact", {}) or {}
        contact = {
            "name":  contact_raw.get("name"),
            "role":  (
                contact_raw.get("role")
                or contact_raw.get("position")
                or contact_raw.get("title")
            ),
            "email": contact_raw.get("email"),
        }
        contact = {k: v for k, v in contact.items() if v is not None}

        emp_raw = insured_details.get("employees", {}) or {}
        insured = {
            "named_insured":      insured_details.get("named_insured"),
            "trading_name":       insured_details.get("trading_name"),
            "company_reg_no":     insured_details.get("company_reg_no"),
            "principal_activity": insured_details.get("principal_activity"),
            "sic_code":           insured_details.get("sic_code"),
            "years_in_operation": insured_details.get("years_in_operation"),
            "annual_turnover_gbp":self._parse_number(insured_details.get("annual_turnover")),
            "employee_count_ft":  emp_raw.get("full_time") if isinstance(emp_raw, dict) else None,
            "employee_count_pt":  emp_raw.get("part_time") if isinstance(emp_raw, dict) else None,
            "contact":            contact if contact else None,
        }
        insured = {k: v for k, v in insured.items() if v is not None}
        flattened["insured"] = insured

        # ----- Policy -----
        policy_period = data.get("policy_period", {}) or {}
        terrorism_raw = data.get("terrorism", "")
        terrorism = None
        if terrorism_raw:
            terrorism = {
                "requested": "requested" in str(terrorism_raw).lower(),
                "pool": "Pool Re" if "Pool Re" in str(terrorism_raw) else None,
            }
            terrorism = {k: v for k, v in terrorism.items() if v is not None}

        excess_obj = data.get("excess", {}) or {}
        excess_std = (
            self._parse_number(excess_obj.get("all_perils"))
            or self._parse_number(excess_obj.get("each_loss"))
            or self._parse_number(excess_obj.get("standard"))
            or self._parse_number(data.get("excess_standard_gbp"))
            or self._parse_number(data.get("excess_each_loss"))
        )

        submitted_by = data.get("submitted_by", {}) or {}
        policy = {
            "submission_ref":       data.get("submission_ref"),
            "class":                data.get("class"),
            "inception_date":       self._parse_date(policy_period.get("start")),
            "expiry_date":          self._parse_date(policy_period.get("end")),
            "policy_period_months": 12,
            "underwriter":          (
                submitted_by.get("name")
                if isinstance(submitted_by, dict)
                else str(submitted_by or "Unknown")
            ),
            "underwriter_division": None,
            "policy_form":          data.get("policy_form"),
            "perils":               data.get("perils", []),
            "excess_standard_gbp":  excess_std,
            "excess_flood_gbp":     self._parse_number(excess_obj.get("flood")),
            "terrorism":            terrorism,
        }
        policy = {k: v for k, v in policy.items() if v is not None}
        flattened["policy"] = policy

        # ----- Business Interruption -----
        bi_raw = data.get("business_interruption", {})
        business_interruption = None
        if bi_raw:
            if isinstance(bi_raw, str):
                bi_str      = bi_raw
                bi_included = bi_str.lower().startswith("yes")
                months_m    = re.search(r'(\d+)[- ]month', bi_str, re.IGNORECASE)
                amount_m    = re.search(r'£([\d,]+)', bi_str)
                bi_months   = int(months_m.group(1)) if months_m else None
                bi_gp       = float(amount_m.group(1).replace(',', '')) if amount_m else None
                bi_basis    = "Gross Profit" if bi_gp else None
            else:
                bi          = bi_raw
                bi_included = bi.get("included", bool(bi))
                bi_months   = (
                    bi.get("indemnity_period_months")
                    or bi.get("indemnity_months")
                    or bi.get("indemnity_period")
                )
                bi_gp = self._parse_number(
                    bi.get("gross_profit")
                    or bi.get("gross_profit_gbp")
                    or bi.get("gross_profit_amount")
                    or bi.get("amount")
                    or bi.get("limit")
                    or bi.get("sum_insured")
                    or bi.get("value")
                )
                bi_basis = bi.get("basis") or ("Gross Profit" if bi_gp else None)

            business_interruption = {
                "included":         bi_included,
                "indemnity_months": bi_months,
                "gross_profit_gbp": bi_gp,
                "basis":            bi_basis,
            }
            business_interruption = {k: v for k, v in business_interruption.items() if v is not None}

        # ----- Machinery Breakdown -----
        mb_raw = data.get("machinery_breakdown", {})
        machinery_breakdown = None
        if mb_raw:
            if isinstance(mb_raw, str):
                mb_str      = mb_raw
                mb_included = mb_str.lower().startswith("yes")
                amount_m    = re.search(r'£([\d,]+)', mb_str)
                loc_m       = re.search(r'Loc\.?\s*(\d+)', mb_str, re.IGNORECASE)
                mb_sublimit = float(amount_m.group(1).replace(',', '')) if amount_m else None
                mb_locs     = [f"00{loc_m.group(1)}"] if loc_m else []
            else:
                mb          = mb_raw
                mb_included = mb.get("included", False)
                mb_sublimit = self._parse_number(
                    mb.get("sublimit")
                    or mb.get("sublimit_gbp")
                    or mb.get("limit")
                    or mb.get("amount")
                )
                mb_locs = (
                    [mb.get("location")] if mb.get("location")
                    else mb.get("location_ids", [])
                    or mb.get("locations", [])
                )
            machinery_breakdown = {
                "included":     mb_included,
                "sublimit_gbp": mb_sublimit,
                "location_ids": mb_locs,
            }
            machinery_breakdown = {k: v for k, v in machinery_breakdown.items() if v is not None}

        # ----- Coverage -----
        coverage = {
            "total_building_si_gbp": self._parse_number(
                data.get("total_building_si")
                or data.get("total_building_si_gbp")
            ),
            "total_contents_si_gbp": self._parse_number(
                data.get("total_contents_si")
                or data.get("total_contents_si_gbp")
            ),
            "total_tiv_gbp": self._parse_number(
                data.get("total_tiv")
                or data.get("total_tiv_gbp")
            ),
            "business_interruption": business_interruption,
            "machinery_breakdown":   machinery_breakdown,
        }
        coverage = {k: v for k, v in coverage.items() if v is not None}
        flattened["coverage"] = coverage

        # ----- Locations -----
        # DeepSeek uses "locations" directly OR "location_schedule" — try both
        raw_locations = data.get("location_schedule") or data.get("locations", [])

        locations = []
        for idx, loc in enumerate(raw_locations):
            sqft_val = (
                loc.get("sqft")
                or loc.get("area_sqft")
                or loc.get("floor_area_sqft")
                or loc.get("area")
                or loc.get("gross_internal_area")
            )
            building_si = (
                loc.get("building_si")
                or loc.get("building_si_gbp")
                or loc.get("building_sum_insured")
                or loc.get("building_value")
            )
            contents_si = (
                loc.get("contents_si")
                or loc.get("contents_si_gbp")
                or loc.get("contents_sum_insured")
                or loc.get("contents_value")
            )
            # Auto-generate location_id from index if DeepSeek omits it
            fallback_id = str(idx + 1).zfill(3)  # "001", "002", "003"
            loc_data = {
                "location_id":      (
                    loc.get("loc")
                    or loc.get("location_id")
                    or loc.get("loc_number")
                    or loc.get("id")
                    or fallback_id
                ),
                "address":          loc.get("address"),
                "occupancy":        loc.get("occupancy"),
                "construction":     loc.get("construction"),
                "year_built":       loc.get("year_built"),
                "year_refurbished": (
                    loc.get("refurbishment_year")
                    or loc.get("year_refurbished")
                    or loc.get("year_refurb")
                ),
                "sqft":             sqft_val,
                "building_si_gbp":  self._parse_number(building_si),
                "contents_si_gbp":  self._parse_number(contents_si),
            }
            loc_data = {k: v for k, v in loc_data.items() if v is not None}
            locations.append(loc_data)
        flattened["locations"] = locations

        # Compute coverage totals from locations if missing at top level
        if locations:
            total_building = sum(loc.get("building_si_gbp", 0) for loc in locations)
            total_contents = sum(loc.get("contents_si_gbp", 0) for loc in locations)
            if not flattened["coverage"].get("total_building_si_gbp"):
                flattened["coverage"]["total_building_si_gbp"] = total_building or None
            if not flattened["coverage"].get("total_contents_si_gbp"):
                flattened["coverage"]["total_contents_si_gbp"] = total_contents or None
            if not flattened["coverage"].get("total_tiv_gbp"):
                computed = total_building + total_contents
                flattened["coverage"]["total_tiv_gbp"] = computed if computed > 0 else None

        # ----- Risk Features -----
        risk = data.get("risk_features", {}) or {}
        broker_notes = []
        for key in ("broker_note", "broker_notes"):
            val = risk.get(key)
            if val:
                if isinstance(val, list):
                    broker_notes.extend(val)
                else:
                    broker_notes.append(val)

        risk_features = {
            "sprinkler_system":      self._coerce_to_string(risk.get("sprinkler_system")),
            "fire_alarm":            self._coerce_to_string(risk.get("fire_alarm")),
            "security":              self._coerce_to_string(risk.get("security")),
            "flood_risk":            self._coerce_to_string(risk.get("flood_risk")),
            "hot_works":             self._coerce_to_string(risk.get("hot_works")),
            "storage_of_flammables": self._coerce_to_string(risk.get("storage_of_flammables")),
            "broker_notes":          broker_notes if broker_notes else None,
        }
        risk_features = {k: v for k, v in risk_features.items() if v is not None}
        flattened["risk_features"] = risk_features if risk_features else None

        # ----- Loss History -----
        loss_history = []
        for loss in data.get("loss_history", []):
            loss_data = {
                "date":               self._parse_date(loss.get("date")),
                "location_id":        loss.get("location") or loss.get("location_id"),
                "peril":              loss.get("peril"),
                "gross_incurred_gbp": self._parse_number(
                    loss.get("gross_incurred")
                    or loss.get("gross_incurred_gbp")
                ),
                "net_paid_gbp":       self._parse_number(
                    loss.get("net_paid")
                    or loss.get("net_paid_gbp")
                ),
                "status":             loss.get("status", "").lower(),
            }
            loss_data = {k: v for k, v in loss_data.items() if v is not None}
            loss_history.append(loss_data)
        flattened["loss_history"] = loss_history

        # ----- Documents -----
        documents = []
        for doc in data.get("documents_enclosed", []):
            if isinstance(doc, str):
                appendix_m = re.search(r'\((Appendix [A-Z])\)', doc)
                appendix   = appendix_m.group(1) if appendix_m else ""
                title      = doc.replace(f"({appendix})", "").strip().rstrip("—").strip()
                documents.append({"title": title, "appendix": appendix, "status": "received"})
            elif isinstance(doc, dict):
                documents.append({
                    "title":    doc.get("title"),
                    "appendix": doc.get("appendix", ""),
                    "status":   doc.get("status", "received"),
                })
        flattened["documents_enclosed"] = documents

        return flattened

    def call_deepseek(self, raw_text: str, correlation_id: str = None) -> Dict[str, Any]:
        system_prompt = self._load_system_prompt()
        user_prompt = f"""Document content:
{raw_text}

Extract all submission data from the document above.
Return your response as a json object only. No markdown, no explanation, just json."""

        if correlation_id:
            print(f"🤖 Calling DeepSeek API for: {correlation_id}")
        try:
            response = requests.post(
                self.deepseek_config.endpoint,
                headers={
                    "Authorization": f"Bearer {self.deepseek_config.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.deepseek_config.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": self.deepseek_config.temperature,
                    "response_format": {"type": "json_object"}
                },
                timeout=self.deepseek_config.timeout_seconds
            )
            response.raise_for_status()
            result = response.json()
            if not result.get("choices"):
                error_msg = f"No 'choices' in response: {json.dumps(result, default=str)[:500]}"
                print(f"❌ {error_msg}")
                return {"status": "CLIENT_ERROR", "error_message": error_msg, "data": None}
            content = result["choices"][0].get("message", {}).get("content", "")
            print(f"📝 Response length: {len(content)} chars")
            print(f"📝 Preview: {content[:150]}...")
            if not content or not content.strip():
                finish_reason = result["choices"][0].get("finish_reason", "unknown")
                return {
                    "status": "CLIENT_ERROR",
                    "error_message": f"Empty response. Finish reason: {finish_reason}",
                    "data": None
                }
            extracted = json.loads(content.strip())
            return {"status": "SUCCESS", "error_message": None, "data": extracted}
        except requests.exceptions.Timeout:
            return {"status": "TIMEOUT", "error_message": "DeepSeek API timed out", "data": None}
        except requests.exceptions.RequestException as e:
            error_body = ""
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_body = e.response.text[:500]
                except:
                    pass
            error_type = "CLIENT_ERROR" if (hasattr(e, 'response') and e.response and 400 <= e.response.status_code < 500) else "SERVER_ERROR"
            return {"status": error_type, "error_message": f"API error: {error_body}", "data": None}
        except json.JSONDecodeError as e:
            return {"status": "CLIENT_ERROR", "error_message": f"JSON parse error: {e}", "data": None}
        except Exception as e:
            return {"status": "SERVER_ERROR", "error_message": f"Unexpected: {e}", "data": None}

    def _load_system_prompt(self) -> str:
        prompt_version = config.parsing.prompt_version
        prompt_file = Path(f"prompts/system_prompt_{prompt_version}.txt")
        if prompt_file.exists():
            with open(prompt_file, 'r') as f:
                return f.read()
        else:
            return """You are an expert insurance underwriting assistant..."""

    def extract_text_from_pdf(self, pdf_path: Path) -> str:
        doc = fitz.open(pdf_path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        if not text.strip():
            print("⚠️ No text found with PyMuPDF – falling back to OCR...")
            try:
                images = convert_from_path(pdf_path, dpi=300)
                print(f"📄 Converted {len(images)} pages to images")
                ocr_text = ""
                for i, img in enumerate(images):
                    print(f"  🔍 OCR page {i+1}/{len(images)}...")
                    page_text = pytesseract.image_to_string(img, lang='eng')
                    ocr_text += f"\n--- Page {i+1} ---\n" + page_text
                text = ocr_text
                print(f"✅ OCR extracted {len(text)} characters")
            except Exception as e:
                print(f"❌ OCR failed: {e}")
                return ""
        return text

    def validate_and_store_artifact(self, correlation_id: str, extracted_dict: Dict[str, Any]) -> bool:
        try:
            print("\n📊 EXTRACTED DATA STRUCTURE:")
            print(f"  Keys: {list(extracted_dict.keys())}")
            if 'insured' in extracted_dict:
                print(f"  Insured keys: {list(extracted_dict['insured'].keys())}")
            if 'policy' in extracted_dict:
                print(f"  Policy keys: {list(extracted_dict['policy'].keys())}")
            if 'locations' in extracted_dict:
                print(f"  Locations count: {len(extracted_dict['locations'])}")
                if extracted_dict['locations']:
                    print(f"  First location keys: {list(extracted_dict['locations'][0].keys())}")

            extracted_dict["correlation_id"] = correlation_id
            extracted_dict["schema_version"] = config.parsing.prompt_version
            extracted_dict["timestamp_extracted"] = datetime.now().isoformat()

            if "source_file_hash" not in extracted_dict or not extracted_dict["source_file_hash"]:
                import hashlib
                content = json.dumps(extracted_dict, sort_keys=True)
                extracted_dict["source_file_hash"] = hashlib.sha256(content.encode()).hexdigest()
                print(f"  ✅ Added source_file_hash: {extracted_dict['source_file_hash'][:16]}...")

            print("\n🔍 Attempting Pydantic validation...")
            artifact = UnderwritingSubmission(**extracted_dict)
            print("  ✅ Validation passed!")

            self.db_store.store_artifact(
                correlation_id,
                artifact.model_dump(),
                is_valid=True
            )
            return True
        except Exception as e:
            print(f"❌ Validation error: {e}")
            debug_path = self.artifact_dir / correlation_id / "validation_error.json"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            with open(debug_path, 'w') as f:
                json.dump({
                    "error": str(e),
                    "data": extracted_dict
                }, f, indent=2, default=str)
            print(f"💾 Validation error saved to: {debug_path}")
            self.db_store.store_artifact(
                correlation_id,
                extracted_dict,
                is_valid=False
            )
            return False

    def save_parsed_json(self, correlation_id: str, data: Dict[str, Any]) -> Path:
        artifact_path = self.artifact_dir / correlation_id
        artifact_path.mkdir(exist_ok=True)
        json_path = artifact_path / f"{correlation_id}.json"
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        return json_path

    def send_to_kafka_handoff(self, correlation_id: str):
        print(f"🔄 Sending to handoff topic: {config.kafka.handoff_topic}")
        self.kafka_handoff.send(
            artifact_id=correlation_id,
            schema_version=config.parsing.prompt_version
        )
        print(f"✅ Handoff message sent for: {correlation_id}")

    def send_output_to_kafka(self, correlation_id: str, status: str,
                             error_message: Optional[str] = None,
                             parsed_json_path: Optional[str] = None):
        message = {
            "correlation_id": correlation_id,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        if error_message:
            message["error_message"] = error_message
        if parsed_json_path:
            message["parsed_json_path"] = parsed_json_path
        self.producer.produce(
            config.kafka.parser_output_topic,
            key=correlation_id.encode('utf-8'),
            value=json.dumps(message).encode('utf-8'),
            callback=self._delivery_report
        )
        self.producer.flush()

    def process_message(self, message: Dict[str, Any]):
        correlation_id = message["correlation_id"]
        file_path = message["file_path"]
        with self.tracer.start_as_current_span("parser_worker.process") as span:
            span.set_attribute("correlation_id", correlation_id)
            print(f"\n📥 Processing: {correlation_id}")
            try:
                pdf_path = Path(file_path)
                if not pdf_path.exists():
                    raise FileNotFoundError(f"PDF not found: {pdf_path}")

                self.db_store.update_submission_status(correlation_id, "PARSING")
                print(f"📊 Status: PARSING")

                raw_text = self.extract_text_from_pdf(pdf_path)
                if not raw_text or len(raw_text.strip()) == 0:
                    error_message = "No text extracted from PDF"
                    self.db_store.update_submission_status(
                        correlation_id,
                        "FAILED",
                        error_type="EMPTY_RESULT",
                        error_message=error_message
                    )
                    self.send_output_to_kafka(correlation_id, "EMPTY_RESULT", error_message=error_message)
                    span.set_status(Status(StatusCode.ERROR, error_message))
                    return

                print(f"📄 Extracted {len(raw_text)} characters of text")
                result = self.call_deepseek(raw_text, correlation_id)

                status = result["status"]
                error_message = result.get("error_message")
                parsed_json_path = None

                if status == "SUCCESS":
                    print(f"✅ DeepSeek extraction successful")
                    deepseek_response = result["data"]

                    debug_path = self.artifact_dir / correlation_id / "deepseek_full_response.json"
                    debug_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(debug_path, 'w') as f:
                        json.dump(deepseek_response, f, indent=2)
                    print(f"💾 Full DeepSeek response saved to: {debug_path}")

                    # ----- Try Azure flattening first -----
                    flattened_data = self._flatten_azure_response(deepseek_response)

                    # If critical fields are missing, fall back to flat flattening
                    if (not flattened_data.get("insured") or not flattened_data.get("policy") or
                        not flattened_data.get("coverage") or not flattened_data.get("locations")):
                        print("⚠️ Azure flattening produced empty required fields – falling back to flat mapping.")
                        flattened_data = self._flatten_flat_response(deepseek_response)

                    flattened_data["correlation_id"] = correlation_id
                    flattened_data["schema_version"] = config.parsing.prompt_version
                    flattened_data["timestamp_extracted"] = datetime.now().isoformat()

                    self.db_store.store_deepseek_response(
                        correlation_id,
                        deepseek_response,
                        is_valid=True
                    )

                    is_valid = self.validate_and_store_artifact(correlation_id, flattened_data)

                    if is_valid:
                        json_path = self.save_parsed_json(correlation_id, flattened_data)
                        parsed_json_path = str(json_path)
                        self.db_store.update_submission_status(
                            correlation_id,
                            "COMPLETED",
                            parsed_json_path=parsed_json_path
                        )
                        print(f"✅ Validation successful, saved to {parsed_json_path}")
                        self.send_to_kafka_handoff(correlation_id)
                        self.db_store.mark_completed(correlation_id)
                        print(f"✅ Processing complete for: {correlation_id}")
                    else:
                        status = "VALIDATION_FAILED"
                        error_message = "Schema validation failed"
                        print(f"❌ Validation failed")
                        self.db_store.update_submission_status(
                            correlation_id,
                            "FAILED",
                            error_type=status,
                            error_message=error_message
                        )
                else:
                    print(f"❌ DeepSeek failed: {status} - {error_message}")
                    self.db_store.update_submission_status(
                        correlation_id,
                        "FAILED",
                        error_type=status,
                        error_message=error_message
                    )

                self.send_output_to_kafka(
                    correlation_id,
                    status,
                    error_message=error_message,
                    parsed_json_path=parsed_json_path
                )
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                error_message = f"Unexpected error: {str(e)}"
                print(f"❌ Unexpected error: {error_message}")
                import traceback
                traceback.print_exc()
                self.db_store.update_submission_status(
                    correlation_id,
                    "FAILED",
                    error_type="UNEXPECTED",
                    error_message=error_message
                )
                self.send_output_to_kafka(correlation_id, "UNEXPECTED", error_message=error_message)
                span.set_status(Status(StatusCode.ERROR, str(e)))

    def run(self):
        print("\n" + "="*60)
        print("🚀 Parser Worker Started (with OCR support)")
        print(f"📡 Consuming from: {config.kafka.parser_input_topic}")
        print(f"📤 Sending results to: {config.kafka.parser_output_topic}")
        print(f"🔄 Handoff topic: {config.kafka.handoff_topic}")
        print("="*60 + "\n")
        print("Press Ctrl+C to stop\n")

        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"❌ Consumer error: {msg.error()}")
                    continue
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self.process_message(message)
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
        except KeyboardInterrupt:
            print("\n🛑 Interrupted by user")
        finally:
            self.consumer.close()
            print("✅ Consumer closed")

