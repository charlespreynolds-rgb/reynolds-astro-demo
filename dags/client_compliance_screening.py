from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    default_args={"owner": "charles", "retries": 1},
    tags=["banking", "compliance", "kyc", "aml"],
)
def client_compliance_screening():

    @task
    def check_kyc() -> dict:
        print("Running KYC verification — confirming customer identity")
        return {"check": "KYC", "status": "PASSED", "score": 95}

    @task
    def check_aml() -> dict:
        print("Running AML screening — checking against sanctions lists")
        return {"check": "AML", "status": "PASSED", "risk": "Low"}

    @task
    def check_credit_bureau() -> dict:
        print("Running credit bureau pull — Experian, Equifax, TransUnion")
        return {"check": "Credit Bureau", "status": "PASSED", "score": 720}

    @task
    def check_fraud_score() -> dict:
        print("Running fraud model score — behavioral analytics check")
        return {"check": "Fraud Score", "status": "PASSED", "score": 98}

    check_kyc()
    check_aml()
    check_credit_bureau()
    check_fraud_score()

client_compliance_screening()