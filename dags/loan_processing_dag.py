from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args={"owner": "charles", "retries": 2},
    tags=["banking", "lending"],
)
def loan_processing_pipeline():

    @task
    def ingest_application() -> dict:
        print("Step 1: New loan application received from borrower portal")
        return {"applicant": "ABC Corp", "amount": 500000, "type": "SBA"}

    @task
    def validate_documents(application: dict) -> dict:
        print(f"Step 2: Validating documents for {application['applicant']}")
        application["documents_verified"] = True
        return application

    @task
    def run_credit_check(application: dict) -> dict:
        print(f"Step 3: Running credit check for {application['applicant']}")
        application["credit_score"] = 720
        application["risk_rating"] = "Low"
        return application

    @task
    def generate_decision(application: dict) -> None:
        print(f"Step 4: Generating underwriting decision for {application['applicant']}")
        print(f"Credit Score: {application['credit_score']}, Risk: {application['risk_rating']}")
        print("Decision: APPROVED - Loan proceeds to closing")

    app = ingest_application()
    validated = validate_documents(app)
    checked = run_credit_check(validated)
    generate_decision(checked)

loan_processing_pipeline()