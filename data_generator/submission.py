import uuid
import random
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

fake = Faker('en_GB')

def generate_submissions(broker_ids, num_submissions=200):
    submissions = []
    
    # Realistic SIC codes for commercial property
    sic_codes = [
        "25620", "52103", "68209", "25990", "32990",  # manufacturing, warehousing, real estate
        "47190", "56103", "68100", "73110", "82990"
    ]
    
    statuses = ["SUBMITTED", "QUOTED", "BOUND", "DECLINED", "REFERRED"]
    status_weights = [0.40, 0.25, 0.15, 0.10, 0.10]  # realistic pipeline
    
    for _ in range(num_submissions):
        broker_id = random.choice(broker_ids)
        
        insured_name = fake.company()
        company_number = fake.bban()[:8] + str(random.randint(0,9))
        industry_code = random.choice(sic_codes)
        annual_turnover = round(random.uniform(500_000, 30_000_000), -3)
        employee_count = random.randint(3, 500)
        
        # inception could be in the past or future
        inception_date = fake.date_between(start_date="-2y", end_date="+3m")
        policy_start = datetime(2026, 7, 1).date()
        policy_end = datetime(2027, 6, 30).date()
        
        # Placeholder sums – will be updated after locations
        total_building_si = 0
        total_contents_si = 0
        total_bi_si = round(random.uniform(200_000, 5_000_000), -3)
        total_tiv = 0
        
        status = random.choices(statuses, weights=status_weights)[0]
        
        created_at = fake.date_time_between(start_date="-60d", end_date="now")
        updated_at = created_at + timedelta(days=random.randint(0, 20))
        
        submissions.append({
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
            "total_building_si": total_building_si,
            "total_contents_si": total_contents_si,
            "total_bi_si": total_bi_si,
            "total_tiv": total_tiv,
            "status": status,
            "created_at": created_at,
            "updated_at": updated_at
        })
    
    return submissions

data_dir = '/home/lang-chain/Documents/underwriting_assistant/raw_data'
brokers_df = pd.read_csv(f"{data_dir}/brokers.csv")
broker_ids = brokers_df["broker_id"].tolist()

submissions_df = pd.DataFrame(generate_submissions(broker_ids, 200))


submissions_df.to_csv(f"{data_dir}/submissions.csv", index=False)

print(f"Generated {len(submissions_df)} submissions")
print(submissions_df.head(3))