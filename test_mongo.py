# check_error.py
import sys
sys.path.insert(0, '.')

from src.underwriter_parser.mongodb_storage import MongoDBSubmissionStore

store = MongoDBSubmissionStore()
correlation_id = "20260622131413-b89916f0"

record = store.get_submission_record(correlation_id)
print("📊 Submission Record:")
print(f"  Status: {record.get('status')}")
print(f"  Error Type: {record.get('error_type')}")
print(f"  Error Message: {record.get('error_message')}")
print(f"  File Path: {record.get('file_path')}")