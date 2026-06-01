import uuid
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
import numpy as np

fake = Faker('en_GB')
np.random.seed(42)
random.seed(42)

def generate_brokers(num_brokers=100):
    brokerage_pool = [
        "Meridian Risk Brokers Ltd.", "Gallagher UK", "Marsh Commercial", "Aon UK",
        "Howden Insurance Brokers", "Lockton Companies LLP", "WTW (Willis Towers Watson)",
        "JLT Specialty", "Benchmark Insurance", "NFP UK", "Griffiths & Armour",
        "Tysers Insurance", "Bluefin Insurance", "Sutton Winson", "Clements & Co.",
        "Arthur J. Gallagher", "Brown & Brown UK", "Hyperion Insurance", "PIB Group",
        "GRP (Global Risk Partners)", "Premier Insurance", "Direct Insurance Brokers",
        "City Insurance Partners", "Ashley Page Insurance", "Bollington Insurance",
        "A-Plan Insurance", "Swinton Commercial", "Hiscox Broker", "Allianz Commercial Partners"
    ]
    brokers = []
    for _ in range(num_brokers):
        broker_name = fake.name()
        brokerage = random.choice(brokerage_pool)
        email = f"{broker_name.lower().replace(' ', '.')}@{brokerage.split()[0].lower()}.co.uk"
        if random.random() > 0.7:
            email = email.replace(".co.uk", ".com")
        phone = fake.phone_number()
        annual_premium_volume = round(random.uniform(1_000_000, 200_000_000), -3)
        loss_ratio = round(random.uniform(0.40, 0.90), 3)
        trust_score = round(random.uniform(55, 99), 1)
        created_at = fake.date_time_between(start_date="-5y", end_date="-1d")
        updated_at = created_at + timedelta(days=random.randint(0, 365))
        brokers.append({
            "broker_id": str(uuid.uuid4()),
            "broker_name": broker_name,
            "brokerage_name": brokerage,
            "email": email,
            "phone": phone,
            "annual_premium_volume": annual_premium_volume,
            "loss_ratio": loss_ratio,
            "trust_score": trust_score,
            "created_at": created_at,
            "updated_at": updated_at
        })
    return pd.DataFrame(brokers)

def generate_submissions(broker_ids, num_submissions=200):
    sic_codes = ["25620","52103","68209","25990","32990","47190","56103","68100","73110","82990"]
    statuses = ["SUBMITTED","QUOTED","BOUND","DECLINED","REFERRED"]
    status_weights = [0.40,0.25,0.15,0.10,0.10]
    subs = []
    for _ in range(num_submissions):
        broker_id = random.choice(broker_ids)
        insured_name = fake.company()
        company_number = fake.bban()[:8] + str(random.randint(0,9))
        industry_code = random.choice(sic_codes)
        annual_turnover = round(random.uniform(500_000, 30_000_000), -3)
        employee_count = random.randint(3, 500)
        inception_date = fake.date_between(start_date="-2y", end_date="+3m")
        policy_start = datetime(2026,7,1).date()
        policy_end = datetime(2027,6,30).date()
        total_bi_si = round(random.uniform(200_000, 5_000_000), -3)
        status = random.choices(statuses, weights=status_weights)[0]
        created_at = fake.date_time_between(start_date="-60d", end_date="now")
        updated_at = created_at + timedelta(days=random.randint(0,20))
        subs.append({
            "submission_id": str(uuid.uuid4()),
            "broker_id": broker_id,
            "insured_name": insured_name,
            "company_number": company_number,
            "industry_code": industry_code,
            "annual_turnover": annual_turnover,
            "employee_count": employee_count,
            "inception_date": inception_date,
            "policy_start": policy_start,
            "policy_end": policy_end,
            "total_building_si": 0,  # placeholder
            "total_contents_si": 0,
            "total_bi_si": total_bi_si,
            "total_tiv": 0,
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at
        })
    return pd.DataFrame(subs)


OCCUPANCIES = {
    "MANUFACTURING": {"hazard_score":4.2,"base_rate":0.0185},
    "WAREHOUSE": {"hazard_score":3.1,"base_rate":0.0120},
    "OFFICE": {"hazard_score":1.8,"base_rate":0.0075},
    "RETAIL": {"hazard_score":2.2,"base_rate":0.0090},
    "LABORATORY": {"hazard_score":3.5,"base_rate":0.0140}
}
CONSTRUCTIONS = {
    "STEEL_FRAME": {"combustibility":3.5,"fire_rating":0.0080},
    "MASONRY": {"combustibility":2.2,"fire_rating":0.0065},
    "PORTAL_FRAME": {"combustibility":4.0,"fire_rating":0.0095},
    "TIMBER": {"combustibility":5.0,"fire_rating":0.0120},
    "CONCRETE": {"combustibility":1.5,"fire_rating":0.0055}
}
CITIES = ["London","Birmingham","Manchester","Leeds","Bristol","Derby","Nottingham","Leicester","Sheffield","Liverpool"]

def generate_locations(submissions_df):
    locs = []
    for _, sub in submissions_df.iterrows():
        sub_id = sub["submission_id"]
        num = random.choices([1,2,3], weights=[0.4,0.45,0.15])[0]
        for i in range(num):
            occ = random.choice(list(OCCUPANCIES.keys()))
            const = random.choice(list(CONSTRUCTIONS.keys()))
            sqft = random.randint(5000,80000)
            building_si = round(sqft * random.uniform(80,200), -3)
            if occ == "WAREHOUSE":
                contents_ratio = random.uniform(0.8,1.8)
            elif occ == "MANUFACTURING":
                contents_ratio = random.uniform(0.6,1.3)
            elif occ == "OFFICE":
                contents_ratio = random.uniform(0.3,0.9)
            else:
                contents_ratio = random.uniform(0.4,1.2)
            contents_si = round(building_si * contents_ratio, -3)
            year_built = random.randint(1900,2023)
            street = fake.street_address()
            city = random.choice(CITIES)
            postcode = fake.postcode()
            if random.random() > 0.7:
                postcode = f"{random.choice(['DE','NG','B','M','L','S','BS'])}{random.randint(1,99)} {random.randint(1,9)}{random.choice(['AB','BD','GH','JL','NP'])}"
            locs.append({
                "location_id": str(uuid.uuid4()),
                "submission_id": sub_id,
                "location_number": f"LOC-{i+1:03d}",
                "address_line1": street,
                "city": city,
                "postcode": postcode,
                "latitude": round(random.uniform(50.7,55.8),6),
                "longitude": round(random.uniform(-2.5,0.8),6),
                "occupancy_code": occ,
                "construction_code": const,
                "year_built": year_built,
                "square_feet": sqft,
                "building_si": building_si,
                "contents_si": contents_si,
                "created_at": datetime.now()
            })
    return pd.DataFrame(locs)

def update_submission_totals(submissions_df, locations_df):
    # Sum building and contents per submission
    building_sum = locations_df.groupby("submission_id")["building_si"].sum()
    contents_sum = locations_df.groupby("submission_id")["contents_si"].sum()
    submissions_df["total_building_si"] = submissions_df["submission_id"].map(building_sum).fillna(0)
    submissions_df["total_contents_si"] = submissions_df["submission_id"].map(contents_sum).fillna(0)
    submissions_df["total_tiv"] = submissions_df["total_building_si"] + submissions_df["total_contents_si"] + submissions_df["total_bi_si"]
    return submissions_df

def generate_location_risk_data(locations_df):
    risk = []
    for _, loc in locations_df.iterrows():
        pc_prefix = loc["postcode"][0] if loc["postcode"] else "N"
        flood_zone = random.choices(["Zone 1","Zone 2","Zone 3"], weights=[0.7,0.25,0.05])[0]
        flood_score = {"Zone 1":10, "Zone 2":50, "Zone 3":90}[flood_zone]
        subsidence_score = random.randint(0,100)
        crime_score = random.randint(0,100)
        windstorm_score = random.randint(0,100)
        # Postcode risk band A-E
        postcode_risk_band = random.choices(["A","B","C","D","E"], weights=[0.1,0.2,0.4,0.2,0.1])[0]
        catastrophe_score = random.randint(0,100)
        risk.append({
            "risk_id": str(uuid.uuid4()),
            "location_id": loc["location_id"],
            "flood_zone": flood_zone,
            "flood_score": flood_score,
            "subsidence_score": subsidence_score,
            "crime_score": crime_score,
            "windstorm_score": windstorm_score,
            "postcode_risk_band": postcode_risk_band,
            "catastrophe_score": catastrophe_score,
            "source_name": "EA/Internal Model",
            "last_updated": datetime.now()
        })
    return pd.DataFrame(risk)

def generate_protection_features(locations_df):
    prot = []
    for _, loc in locations_df.iterrows():
        occ = loc["occupancy_code"]
        has_sprinkler = (occ == "MANUFACTURING") or (occ == "WAREHOUSE" and random.random() < 0.3)
        sprinkler_type = "Wet pipe" if has_sprinkler else None
        bafsa_certified = has_sprinkler and random.random() > 0.3
        fire_alarm = True
        monitored_alarm = random.random() > 0.2
        cctv = random.random() > 0.3
        perimeter_fencing = occ != "OFFICE"
        intruder_alarm = random.random() > 0.4
        hot_work_permit = occ == "MANUFACTURING"
        prot.append({
            "protection_id": str(uuid.uuid4()),
            "location_id": loc["location_id"],
            "sprinkler_present": has_sprinkler,
            "sprinkler_type": sprinkler_type,
            "bafsa_certified": bafsa_certified,
            "fire_alarm": fire_alarm,
            "monitored_alarm": monitored_alarm,
            "cctv": cctv,
            "perimeter_fencing": perimeter_fencing,
            "intruder_alarm": intruder_alarm,
            "hot_work_permit": hot_work_permit,
            "updated_at": datetime.now()
        })
    return pd.DataFrame(prot)

ROOF_CONDITIONS = ["Good","Average","Poor","Fair","Excellent"]
ELECTRICAL_RATINGS = ["Good","Fair","Poor","Requires upgrade"]
def generate_survey_findings(locations_df):
    survey = []
    for _, loc in locations_df.iterrows():
        roof = random.choices(ROOF_CONDITIONS, weights=[0.4,0.3,0.1,0.15,0.05])[0]
        electrical = random.choice(ELECTRICAL_RATINGS)
        housekeeping = round(random.uniform(50,100),1)
        overall_grade = random.choices(["A","B","C","D"], weights=[0.1,0.4,0.35,0.15])[0]

        recommendations = f"LLM: generate based on roof={roof}, electrical={electrical}, grade={overall_grade}"
        survey.append({
            "survey_id": str(uuid.uuid4()),
            "location_id": loc["location_id"],
            "survey_date": fake.date_between(start_date="-18m", end_date="now"),
            "roof_condition": roof,
            "electrical_rating": electrical,
            "housekeeping_score": housekeeping,
            "overall_grade": overall_grade,
            "recommendations": recommendations,
            "survey_provider": random.choice(["RGA Risk Survey", "JBA Consulting", "Aon Risk Services", "Marsh Risk Consulting"])
        })
    return pd.DataFrame(survey)

PERILS = ["Fire","Flood","Storm","Escape of water","Machinery breakdown","Theft","Malicious damage","Subsidence"]
CLAIM_STATUSES = ["Closed","Open","Withdrawn","Denied"]
def generate_claims(submissions_df, locations_df, years_back=5):
    claims = []
    # For each submission, 0-3 claims
    for _, sub in submissions_df.iterrows():
        sub_id = sub["submission_id"]
        sub_locs = locations_df[locations_df["submission_id"] == sub_id]
        if sub_locs.empty:
            continue
        num_claims = random.choices([0,1,2,3], weights=[0.55,0.25,0.15,0.05])[0]
        for _ in range(num_claims):
            location = sub_locs.sample(1).iloc[0]
            loss_date = fake.date_between(start_date=f"-{years_back}y", end_date="-30d")
            peril = random.choice(PERILS)
            gross = round(random.uniform(2000, 150000), -2)
            paid = round(gross * random.uniform(0,1), -2) if random.random() > 0.2 else 0
            status = random.choices(CLAIM_STATUSES, weights=[0.7,0.2,0.05,0.05])[0]
            reserve = round(gross - paid, -2) if status == "Open" else 0
            description = f"LLM: generate realistic description for {peril} claim of £{gross:,.0f} at {location['occupancy_code']}"
            claims.append({
                "claim_id": str(uuid.uuid4()),
                "submission_id": sub_id,
                "location_id": location["location_id"],
                "loss_date": loss_date,
                "peril": peril,
                "gross_incurred": gross,
                "paid_amount": paid,
                "reserve_amount": reserve,
                "claim_status": status,
                "description": description
            })
    return pd.DataFrame(claims)


def compute_claim_metrics(claims_df, submissions_df):
    metrics = []
    for _, sub in submissions_df.iterrows():
        sub_id = sub["submission_id"]
        sub_claims = claims_df[claims_df["submission_id"] == sub_id]
        num_claims = len(sub_claims)
        total_paid = sub_claims["paid_amount"].sum() if num_claims > 0 else 0
        avg_severity = sub_claims["gross_incurred"].mean() if num_claims > 0 else 0
        total_tiv = sub["total_tiv"] if sub["total_tiv"] > 0 else 1
        loss_ratio = (total_paid / total_tiv) if total_tiv > 0 else 0
        frequency_score = min(num_claims * 20, 100)
        severity_score = min((avg_severity / 50000) * 100, 100) if avg_severity > 0 else 0
        metrics.append({
            "metric_id": str(uuid.uuid4()),
            "submission_id": sub_id,
            "claims_5yr": num_claims,
            "total_paid_5yr": total_paid,
            "frequency_score": frequency_score,
            "severity_score": severity_score,
            "loss_ratio": round(loss_ratio, 4),
            "calculated_at": datetime.now()
        })
    return pd.DataFrame(metrics)

def create_underwriting_rules():
    rules = [
        {"rule_id": str(uuid.uuid4()), "rule_name": "Flood Zone 3 Decline", "rule_category": "decline",
         "priority": 100, "condition_json": '{"flood_zone": "Zone 3"}', "action_json": '{"decision":"DECLINE"}', "active": True},
        {"rule_id": str(uuid.uuid4()), "rule_name": "Flat Roof Sublimit", "rule_category": "sublimit",
         "priority": 90, "condition_json": '{"roof_condition": ["Poor","Fair"]}', "action_json": '{"sublimit":250000,"per_occurrence":true}', "active": True},
        {"rule_id": str(uuid.uuid4()), "rule_name": "Sprinkler Credit", "rule_category": "credit",
         "priority": 80, "condition_json": '{"bafsa_certified": true}', "action_json": '{"credit_percent": -12.5}', "active": True},
        {"rule_id": str(uuid.uuid4()), "rule_name": "Flood Zone 2 Loading", "rule_category": "loading",
         "priority": 70, "condition_json": '{"flood_zone": "Zone 2"}', "action_json": '{"loading_percent": 6.0}', "active": True},
        {"rule_id": str(uuid.uuid4()), "rule_name": "No Sprinkler High Contents", "rule_category": "referral",
         "priority": 95, "condition_json": '{"contents_si": 3000000, "sprinkler_present": false}', "action_json": '{"referral": true, "reason": "Contents >£3M without sprinkler"}', "active": True},
    ]
    return pd.DataFrame(rules)

def generate_referrals(submissions_df):
    referrals = []
    for _, sub in submissions_df.iterrows():
        if sub["status"] == "REFERRED" or random.random() < 0.15:
            reasons = ["Flat roof without warranty", "Flood Zone 2 with high contents", "Poor loss history", "High TIV > £15M", "Missing survey report"]
            referrals.append({
                "referral_id": str(uuid.uuid4()),
                "submission_id": sub["submission_id"],
                "referral_reason": random.choice(reasons),
                "severity": random.choice(["Low","Medium","High"]),
                "assigned_team": random.choice(["Commercial Property", "Risk Engineering", "Compliance"]),
                "status": random.choice(["Open","In Review","Resolved"]),
                "created_at": datetime.now()
            })
    return pd.DataFrame(referrals)

def generate_requirements(submissions_df):
    req_types = ["Machinery breakdown schedule", "Roof warranty certificate", "Loss runs last 5 years", "Survey report", "Sprinkler maintenance log", "Security system certificate"]
    reqs = []
    for _, sub in submissions_df.iterrows():
        if random.random() < 0.4: 
            num_reqs = random.randint(1,3)
            for _ in range(num_reqs):
                req_type = random.choice(req_types)
                due_date = datetime.now() + timedelta(days=random.randint(7,45))
                status = random.choices(["Pending","Received","Overdue"], weights=[0.7,0.2,0.1])[0]
                reqs.append({
                    "requirement_id": str(uuid.uuid4()),
                    "submission_id": sub["submission_id"],
                    "requirement_type": req_type,
                    "due_date": due_date.date(),
                    "status": status,
                    "received_date": datetime.now().date() if status == "Received" else None,
                    "notes": f"LLM: generate note for {req_type}" if status == "Pending" else None
                })
    return pd.DataFrame(reqs)

def generate_policy_conditions(submissions_df, locations_df, rules_df):
    conditions = []
    for _, loc in locations_df.iterrows():
        sub_id = loc["submission_id"]

        if random.random() < 0.3:
            conditions.append({
                "condition_id": str(uuid.uuid4()),
                "submission_id": sub_id,
                "trigger_rule_id": rules_df.iloc[1]["rule_id"],  # Flat roof rule
                "condition_text": "Annual roof inspection by certified engineer required.",
                "condition_type": "Warranty"
            })
        if random.random() < 0.25:
            conditions.append({
                "condition_id": str(uuid.uuid4()),
                "submission_id": sub_id,
                "trigger_rule_id": rules_df.iloc[3]["rule_id"],
                "condition_text": "Flood sublimit £250,000 any one occurrence.",
                "condition_type": "Sublimit"
            })
    return pd.DataFrame(conditions)


def generate_quotes(submissions_df):
    quotes = []
    for _, sub in submissions_df.iterrows():
        if sub["status"] in ["QUOTED","BOUND"] and sub["total_tiv"] > 0:
            base_premium = round(sub["total_tiv"] * random.uniform(0.008, 0.015), -2)
            loading = round(base_premium * random.uniform(-0.1, 0.2), -2)
            credit = round(base_premium * random.uniform(0, 0.15), -2)
            final_premium = base_premium + loading - credit
            excess = random.choice([10000, 25000, 50000])
            quotes.append({
                "quote_id": str(uuid.uuid4()),
                "submission_id": sub["submission_id"],
                "base_premium": base_premium,
                "loadings": loading,
                "credits": credit,
                "final_premium": final_premium,
                "excess_amount": excess,
                "decision": sub["status"],
                "quote_date": datetime.now()
            })
    return pd.DataFrame(quotes)

def generate_historical_decisions(submissions_df, locations_df, claim_metrics_df):
    decisions = []
    for _, sub in submissions_df.iterrows():
        if sub["status"] in ["BOUND","DECLINED","QUOTED"]:
            sub_locs = locations_df[locations_df["submission_id"] == sub["submission_id"]]
            if sub_locs.empty:
                continue
            occ_codes = sub_locs["occupancy_code"].unique()
            const_codes = sub_locs["construction_code"].unique()
            flood_zones = []  
            flood_zone = random.choice(["Zone 1","Zone 2","Zone 3"])
            claim_metrics_row = claim_metrics_df[claim_metrics_df["submission_id"] == sub["submission_id"]]
            freq_score = claim_metrics_row["frequency_score"].iloc[0] if not claim_metrics_row.empty else 50
            tiv_band = "£5M-10M" if sub["total_tiv"] < 10_000_000 else "£10M-15M" if sub["total_tiv"] < 15_000_000 else "£15M+"
            decision = sub["status"]
            loading_pct = round(random.uniform(-10,25),1)
            excess_amount = random.choice([10000,25000,50000])
            underwriter_comments = f"LLM: generate comment for {decision} based on flood_zone={flood_zone}, freq_score={freq_score}"
            decisions.append({
                "decision_id": str(uuid.uuid4()),
                "occupancy_code": occ_codes[0] if len(occ_codes) > 0 else "UNKNOWN",
                "construction_code": const_codes[0] if len(const_codes) > 0 else "UNKNOWN",
                "flood_zone": flood_zone,
                "claim_frequency_score": freq_score,
                "tiv_band": tiv_band,
                "decision": decision,
                "loading_pct": loading_pct,
                "excess_amount": excess_amount,
                "underwriter_comments": underwriter_comments,
                "created_at": datetime.now()
            })
    return pd.DataFrame(decisions)

def main():
    data_dir = '/home/lang-chain/Documents/underwriting_assistant/raw_data'
    print("1. Generating brokers...")
    brokers_df = generate_brokers(100)
    brokers_df.to_csv(f"{data_dir}/brokers.csv", index=False)
    
    print("2. Generating submissions...")
    broker_ids = brokers_df["broker_id"].tolist()
    submissions_df = generate_submissions(broker_ids, 200)
    submissions_df.to_csv(f"{data_dir}/submissions.csv", index=False)
    
    print("3. Generating locations...")
    locations_df = generate_locations(submissions_df)
    locations_df.to_csv(f"{data_dir}/locations.csv", index=False)
    
    print("4. Updating submission totals...")
    submissions_df = update_submission_totals(submissions_df, locations_df)
    submissions_df.to_csv(f"{data_dir}/submissions_updated.csv", index=False)
    
    print("5. Generating location risk data...")
    location_risk_df = generate_location_risk_data(locations_df)
    location_risk_df.to_csv(f"{data_dir}/location_risk_data.csv", index=False)
    
    print("6. Generating protection features...")
    protection_df = generate_protection_features(locations_df)
    protection_df.to_csv(f"{data_dir}/protection_features.csv", index=False)
    
    print("7. Generating survey findings...")
    survey_df = generate_survey_findings(locations_df)
    survey_df.to_csv(f"{data_dir}/survey_findings.csv", index=False)
    
    print("8. Generating claims...")
    claims_df = generate_claims(submissions_df, locations_df)
    claims_df.to_csv(f"{data_dir}/claims.csv", index=False)
    
    print("9. Computing claim metrics...")
    claim_metrics_df = compute_claim_metrics(claims_df, submissions_df)
    claim_metrics_df.to_csv(f"{data_dir}/claim_metrics.csv", index=False)
    
    print("10. Creating underwriting rules...")
    rules_df = create_underwriting_rules()
    rules_df.to_csv(f"{data_dir}/underwriting_rules.csv", index=False)
    
    print("11. Generating referrals...")
    referrals_df = generate_referrals(submissions_df)
    referrals_df.to_csv(f"{data_dir}/referrals.csv", index=False)
    
    print("12. Generating requirements...")
    requirements_df = generate_requirements(submissions_df)
    requirements_df.to_csv(f"{data_dir}/requirements.csv", index=False)
    
    print("13. Generating policy conditions...")
    policy_conditions_df = generate_policy_conditions(submissions_df, locations_df, rules_df)
    policy_conditions_df.to_csv(f"{data_dir}/policy_conditions.csv", index=False)
    
    print("14. Generating quotes...")
    quotes_df = generate_quotes(submissions_df)
    quotes_df.to_csv(f"{data_dir}/quotes.csv", index=False)
    
    print("15. Generating historical decisions...")
    historical_df = generate_historical_decisions(submissions_df, locations_df, claim_metrics_df)
    historical_df.to_csv(f"{data_dir}/historical_decisions.csv", index=False)
    
    print("All tables generated successfully!")

if __name__ == "__main__":
    main()