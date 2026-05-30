import uuid
import random
from faker import Faker
from datetime import datetime
import pandas as pd
from datetime import timedelta
import os

fake = Faker('en_US')

def generate_brokers(num_brokers=99):
    brokers = []
    for _ in range(num_brokers):
        brokerage_names = [
            "Meridian Risk Brokers Ltd.",
            "Gallagher UK",
            "Marsh Commercial",
            "Aon UK",
            "Howden Insurance Brokers",
            "Lockton Companies LLP",
            "WTW (Willis Towers Watson)",
            "JLT Specialty",
            "Benchmark Insurance",
            "NFP UK",
            "Griffiths & Armour",
            "Tysers Insurance",
            "Bluefin Insurance",
            "Sutton Winson",
            "Clements & Co.",
            "Hadyn Evans Brokers",
            "Prosegur Insurance Limited",
            "sme Insurance Partners",
            "Regal Brokers Group",
            "Apex Risk Management",
            "Sterling Insurance Brokers",
            "Crown Risk Solutions",
            "Prime Commercial Brokers",
            "Horizon Insurance Partners",
            "Vanguard Risk Brokers",
            "Atlas Insurance Group",
            "Pinnacle Brokerage Ltd.",
            "Summit Risk Advisors",
            "Elite Insurance Brokers",
            "Nexus Commercial Brokers",
            "Compass Risk Solutions",
            "Beacon Insurance Partners",
            "Meridian Commercial Group",
            "Crestwood Brokers Ltd.",
            "Oakwood Insurance Brokers",
            "Riverside Risk Advisors",
            "Highland Insurance Group",
            "Transequity Brokers",
            "Zenith Risk Partners",
            "Alpine Insurance Brokers",
            "Capital Risk Brokers Ltd.",
            "Greenfield Insurance Partners",
            "Silverline Brokerage Group",
            "Northstar Risk Advisors",
            "Coastal Insurance Brokers",
            "Meadowland Risk Solutions",
            "Parkway Insurance Group",
            "Bridgepoint Brokers Ltd.",
            "Sterling Point Insurance",
            "Cornerstone Risk Brokers",
            "Arthur J. Gallagher",
            "Brown & Brown Europe",
            "Towergate Insurance",
            "Jensten Insurance Brokers",
            "Higos Insurance Services",
            "Adrian Flux Insurance",
            "Alan Boswell Group",
            "Bishop Skinner Marine",
            "Bspoke Group",
            "Clegg Gifford",
            "Coversure Insurance Services",
            "County Insurance Services",
            "Endsleigh Insurance",
            "Erskine Murray Insurance Brokers",
            "Farr Insurance Brokers",
            "Gauntlet Group",
            "George Stubbs Insurance",
            "Green Insurance Group",
            "Hamilton Robertson Insurance",
            "Henderson Insurance Brokers",
            "Hiscox Insurance Brokers",
            "Insync Insurance",
            "James Hallam Limited",
            "JLW Insurance",
            "Keystone Insurance Group",
            "Konsileo",
            "Lycetts",
            "Mansfield Insurance",
            "McClarrons Ltd",
            "Miller Insurance Services",
            "Momentum Broker Solutions",
            "Movo Partnership",
            "NFU Mutual Agency",
            "Norton Insurance Brokers",
            "One Broker",
            "Partners&",
            "PIB Insurance Brokers",
            "PolicyBee",
            "Premier Choice Group",
            "Qdos Broker & Underwriting Services",
            "R K Harrison Insurance Services",
            "Ravenhall Risk Solutions",
            "Reich Insurance",
            "Robert Gerrard & Co",
            "Russell Scanlan",
            "Schofield Insurance Brokers",
            "TL Dallas Group",
            "Todd & Cue",
            "Tower Insurance Brokers",
            "Yutree Insurance"
        ]

        broker_name = fake.name()
        brokerage = random.choice(brokerage_names)
        email = f"{broker_name.lower().replace(' ', '.')}@{brokerage.split()[0].lower()}.co.uk"
        phone = fake.phone_number()

        annual_premium_volume = round(random.uniform(5_000_000, 150_000_000), -3) 
        loss_ratio = round(random.uniform(0.45, 0.85), 3)  
        trust_score = round(random.uniform(60, 98), 1)  

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
    return brokers

brokers_df = pd.DataFrame(generate_brokers(100))


data_dir = '/home/lang-chain/Documents/underwriting_assistant/raw_data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
data_path = os.path.join(data_dir, 'brokers.csv')
brokers_df.to_csv(data_path, index=False)

for _, row in brokers_df.iterrows():
    broker_name = row['broker_name'].replace("'", "''")
    brokerage_name = row['brokerage_name'].replace("'", "''")
    email = row['email'].replace("'", "''")
    
    print(f"INSERT INTO brokers (broker_id, broker_name, brokerage_name, email, phone, annual_premium_volume, loss_ratio, trust_score, created_at, updated_at) VALUES ('{row['broker_id']}', '{broker_name}', '{brokerage_name}', '{email}', '{row['phone']}', {row['annual_premium_volume']}, {row['loss_ratio']}, {row['trust_score']}, '{row['created_at']}', '{row['updated_at']}');")

print(f"\nData Shape: {brokers_df.shape}")
print(f"\nFirst 5 rows:\n{brokers_df.head()}")