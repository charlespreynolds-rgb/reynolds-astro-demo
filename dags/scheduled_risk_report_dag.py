from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * 1-5",
    default_args={"owner": "charles", "retries": 2},
    tags=["banking", "risk", "reporting"],
)
def daily_risk_report():

    @task
    def extract_overnight_transactions() -> dict:
        print("Extracting overnight transaction data from core banking system")
        return {"transactions": 47832, "total_value": 284000000, "currency": "USD"}

    @task
    def calculate_risk_exposure(data: dict) -> dict:
        print(f"Calculating risk exposure across {data['transactions']} transactions")
        data["risk_exposure"] = data["total_value"] * 0.02
        data["risk_rating"] = "Low"
        return data

    @task
    def check_regulatory_thresholds(data: dict) -> dict:
        print("Checking against Basel III regulatory thresholds")
        data["basel_compliant"] = True
        data["sar_required"] = False
        return data

    @task
    def publish_risk_report(data: dict) -> None:
        print(f"Publishing daily risk report to executive dashboard")
        print(f"Transactions: {data['transactions']}")
        print(f"Risk Exposure: ${data['risk_exposure']:,.2f}")
        print(f"Basel III Compliant: {data['basel_compliant']}")
        print(f"SAR Filing Required: {data['sar_required']}")
        print("Report delivered to CRO, CFO, and Compliance team")

    transactions = extract_overnight_transactions()
    risk_data = calculate_risk_exposure(transactions)
    compliant_data = check_regulatory_thresholds(risk_data)
    publish_risk_report(compliant_data)

daily_risk_report()