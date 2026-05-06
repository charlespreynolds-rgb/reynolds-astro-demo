from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * 1-5",
    default_args={"owner": "charles", "retries": 2},
    tags=["banking", "lending", "complex"],
)
def corporate_loan_origination():

    # Stage 1 — Application Intake
    @task
    def receive_application() -> dict:
        print("New corporate loan application received")
        return {"applicant": "Meridian Capital LLC", "amount": 25000000, "type": "Corporate Term Loan"}

    @task
    def assign_relationship_manager(application: dict) -> dict:
        print(f"Assigning relationship manager to {application['applicant']}")
        application["rm"] = "Sarah Chen"
        return application

    @task
    def validate_application_completeness(application: dict) -> dict:
        print("Validating all required fields and documents are present")
        application["complete"] = True
        return application

    # Stage 2 — Parallel Compliance Checks
    @task
    def run_kyc(application: dict) -> dict:
        print(f"Running KYC verification for {application['applicant']}")
        return {"check": "KYC", "status": "PASSED"}

    @task
    def run_aml(application: dict) -> dict:
        print(f"Running AML screening for {application['applicant']}")
        return {"check": "AML", "status": "PASSED"}

    @task
    def run_sanctions_check(application: dict) -> dict:
        print("Checking OFAC and global sanctions lists")
        return {"check": "Sanctions", "status": "CLEAR"}

    @task
    def run_pep_check(application: dict) -> dict:
        print("Checking Politically Exposed Persons database")
        return {"check": "PEP", "status": "CLEAR"}

    # Stage 3 — Credit Analysis
    @task
    def pull_credit_bureau(application: dict) -> dict:
        print("Pulling credit bureau report from Experian, Equifax, TransUnion")
        return {"credit_score": 780, "history": "Clean"}

    @task
    def analyze_financial_statements(application: dict) -> dict:
        print("Analyzing 3 years of audited financial statements")
        return {"revenue": 85000000, "ebitda": 12000000, "debt_ratio": 0.35}

    @task
    def calculate_dscr(financials: dict) -> dict:
        print("Calculating Debt Service Coverage Ratio")
        dscr = financials["ebitda"] / (financials["revenue"] * 0.05)
        financials["dscr"] = round(dscr, 2)
        return financials

    @task
    def run_industry_analysis(application: dict) -> dict:
        print("Running industry and market analysis for sector risk assessment")
        return {"industry": "Manufacturing", "sector_risk": "Medium", "market_outlook": "Stable"}

    # Stage 4 — Risk Scoring
    @task
    def compile_compliance_results(kyc: dict, aml: dict, sanctions: dict, pep: dict) -> dict:
        print("Compiling all compliance check results")
        return {"all_checks": [kyc, aml, sanctions, pep], "overall_status": "PASSED"}

    @task
    def generate_risk_score(credit: dict, financials: dict, industry: dict, compliance: dict) -> dict:
        print("Generating composite risk score across all dimensions")
        return {"risk_score": 72, "risk_rating": "BBB", "recommendation": "APPROVE"}

    # Stage 5 — Legal and Structuring
    @task
    def structure_loan_terms(risk: dict, application: dict) -> dict:
        print("Structuring loan terms based on risk profile")
        return {"rate": "SOFR + 225bps", "term": "5 years", "covenants": ["DSCR > 1.25", "Max Leverage 3.5x"]}

    @task
    def legal_review(terms: dict) -> dict:
        print("Legal team reviewing loan agreement and covenants")
        terms["legal_approved"] = True
        return terms

    @task
    def credit_committee_approval(risk: dict, terms: dict) -> dict:
        print("Presenting to credit committee for final approval")
        return {"approved": True, "approver": "Credit Committee", "date": "2025-01-15"}

    # Stage 6 — Booking and Reporting
    @task
    def book_loan(approval: dict, terms: dict) -> dict:
        print("Booking loan in core banking system")
        return {"loan_id": "CL-2025-00847", "status": "BOOKED"}

    @task
    def generate_regulatory_report(loan: dict, compliance: dict) -> None:
        print(f"Generating regulatory report for loan {loan['loan_id']}")
        print("Filing required reports with OCC and Federal Reserve")

    @task
    def update_risk_dashboard(loan: dict, risk: dict) -> None:
        print(f"Updating enterprise risk dashboard with new exposure")
        print(f"New loan {loan['loan_id']} added to portfolio")

    @task
    def notify_executive_team(loan: dict, approval: dict) -> None:
        print(f"Notifying executive team of approved loan {loan['loan_id']}")
        print(f"Approved by: {approval['approver']}")
        print("Notifications sent to CEO, CFO, and CRO")

    # Wire up the dependencies
    app = receive_application()
    app_with_rm = assign_relationship_manager(app)
    validated_app = validate_application_completeness(app_with_rm)

    # Parallel compliance checks
    kyc = run_kyc(validated_app)
    aml = run_aml(validated_app)
    sanctions = run_sanctions_check(validated_app)
    pep = run_pep_check(validated_app)

    # Parallel credit analysis
    credit = pull_credit_bureau(validated_app)
    financials = analyze_financial_statements(validated_app)
    dscr = calculate_dscr(financials)
    industry = run_industry_analysis(validated_app)

    # Converge compliance results
    compliance = compile_compliance_results(kyc, aml, sanctions, pep)

    # Generate composite risk score
    risk = generate_risk_score(credit, dscr, industry, compliance)

    # Structure and approve
    terms = structure_loan_terms(risk, validated_app)
    legal = legal_review(terms)
    approval = credit_committee_approval(risk, legal)

    # Book and report
    loan = book_loan(approval, terms)
    generate_regulatory_report(loan, compliance)
    update_risk_dashboard(loan, risk)
    notify_executive_team(loan, approval)

corporate_loan_origination()