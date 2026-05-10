"""
FX Settlement Optimizer — Astronomer SE Technical Exercise
Charles Reynolds
 
Story: A Tier-1 bank has booked a large cross-border corporate loan requiring
currency settlement. The FX desk needs to optimize when and how to batch the
conversion for the best execution rate. This pipeline monitors live rates,
models historical patterns, detects volatility, checks peer benchmarks,
calculates savings, generates a treasury recommendation, and triggers
the appropriate downstream action — all with a full audit trail in Snowflake.
"""
 
from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import requests
 
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    default_args={"owner": "charles", "retries": 2},
    tags=["banking", "fx", "treasury", "settlement"],
    doc_md=__doc__,
)
def fx_settlement_optimizer():
 
    # ─────────────────────────────────────────────
    # STAGE 1 — DATA INGESTION
    # Pull live rates + load historical baseline
    # ─────────────────────────────────────────────
 
    @task
    def pull_live_fx_rates() -> dict:
        """
        Pulls live FX rates from the Exchange Rate API.
        In production: connects to Bloomberg, Refinitiv, or
        the bank's internal rate feed via secured API gateway.
        """
        try:
            response = requests.get("https://open.er-api.com/v6/latest/USD")
            response.raise_for_status()
            data = response.json()
            rates = data["rates"]
            print(f"Live FX rates pulled successfully. Base: USD")
            print(f"Rate timestamp: {data['time_last_update_utc']}")
            return {
                "base": "USD",
                "timestamp": data["time_last_update_utc"],
                "rates": {
                    "EUR": rates["EUR"],
                    "GBP": rates["GBP"],
                    "JPY": rates["JPY"],
                    "CAD": rates["CAD"],
                    "AUD": rates["AUD"],
                    "CHF": rates["CHF"],
                    "CNY": rates["CNY"],
                    "MXN": rates["MXN"],
                }
            }
        except Exception as e:
            print(f"Live API unavailable — using reference rates: {e}")
            return {
                "base": "USD",
                "timestamp": "2025-01-15T06:00:00Z",
                "rates": {
                    "EUR": 0.9187, "GBP": 0.7923, "JPY": 149.82,
                    "CAD": 1.3641, "AUD": 1.5712, "CHF": 0.8934,
                    "CNY": 7.2341, "MXN": 17.1523,
                }
            }
 
    # Empty operator — represents historical data load from market data warehouse
    load_historical_rates = EmptyOperator(
        task_id="load_historical_rates",
        doc_md="""
        **Production**: Loads 90 days of rate history from the bank's
        market data warehouse (Snowflake/Databricks). Establishes the
        statistical baseline for rate favorability scoring and volatility
        threshold calibration. Without historical context, today's rate
        cannot be evaluated as favorable or unfavorable.
        """
    )
 
    # ─────────────────────────────────────────────
    # STAGE 2 — PARALLEL ANALYSIS
    # Rate optimization + arbitrage + volatility + peer benchmarks
    # All four run simultaneously for speed
    # ─────────────────────────────────────────────
 
    @task
    def identify_optimal_batch_currencies(rate_data: dict) -> dict:
        """
        Identifies currencies with the most favorable rates for batching.
        Compares live rates against historical baseline to calculate
        basis point improvement opportunity.
        """
        rates = rate_data["rates"]
        print(f"Analyzing {len(rates)} currency pairs for batch optimization")
 
        reference_rates = {
            "EUR": 0.9210, "GBP": 0.7950, "JPY": 150.20,
            "CAD": 1.3680, "AUD": 1.5750, "CHF": 0.8960,
            "CNY": 7.2400, "MXN": 17.1800,
        }
 
        improvements = {}
        for currency, current_rate in rates.items():
            ref_rate = reference_rates.get(currency, current_rate)
            bps_improvement = round((current_rate - ref_rate) / ref_rate * 10000, 2)
            improvements[currency] = {
                "current_rate": current_rate,
                "reference_rate": ref_rate,
                "bps_vs_reference": bps_improvement,
                "favorable": bps_improvement > 0
            }
            print(f"{currency}: {current_rate:.4f} vs ref {ref_rate:.4f} = {bps_improvement} bps")
 
        favorable = {k: v for k, v in improvements.items() if v["favorable"]}
        print(f"Found {len(favorable)} currencies with favorable rates")
 
        return {
            "timestamp": rate_data["timestamp"],
            "analysis": improvements,
            "favorable_currencies": list(favorable.keys()),
            "favorable_count": len(favorable)
        }
 
    # Empty operator — represents quant arbitrage model
    run_arbitrage_model = EmptyOperator(
        task_id="run_arbitrage_model",
        doc_md="""
        **Production**: Calls the bank's internal quant pricing model to
        check for triangular arbitrage opportunities across currency pairs.
        At $1.25B daily volume, a 1 basis point improvement from arbitrage
        detection is worth $125,000 per day. Model runs in the bank's
        secure compute environment via Remote Execution Agent.
        """
    )
 
    @task
    def detect_rate_volatility(rate_data: dict) -> dict:
        """
        Flags unusual rate movements indicating market volatility.
        High volatility means batching creates execution risk —
        the pipeline automatically routes to HOLD when unstable.
        """
        rates = rate_data["rates"]
 
        thresholds = {
            "EUR": 0.005, "GBP": 0.006, "JPY": 0.8,
            "CAD": 0.007, "AUD": 0.008, "CHF": 0.005,
            "CNY": 0.010, "MXN": 0.015,
        }
 
        reference_rates = {
            "EUR": 0.9210, "GBP": 0.7950, "JPY": 150.20,
            "CAD": 1.3680, "AUD": 1.5750, "CHF": 0.8960,
            "CNY": 7.2400, "MXN": 17.1800,
        }
 
        volatile_currencies = []
        for currency, current_rate in rates.items():
            ref_rate = reference_rates.get(currency, current_rate)
            movement = abs(current_rate - ref_rate)
            threshold = thresholds.get(currency, 0.01)
            if movement > threshold:
                volatile_currencies.append(currency)
                print(f"VOLATILITY ALERT: {currency} moved {movement:.4f} vs threshold {threshold}")
            else:
                print(f"{currency}: within normal range")
 
        volatility_detected = len(volatile_currencies) > 0
 
        return {
            "volatility_detected": volatility_detected,
            "volatile_currencies": volatile_currencies,
            "market_condition": "VOLATILE" if volatility_detected else "STABLE"
        }
 
    # Empty operator — represents peer bank rate comparison
    check_peer_bank_rates = EmptyOperator(
        task_id="check_peer_bank_rates",
        doc_md="""
        **Production**: Pulls peer bank execution rates from Bloomberg
        or Refinitiv to benchmark our rates against the market.
        Determines whether we are competitive and whether the spread
        justifies batching now versus waiting. Critical for regulatory
        best execution documentation under MiFID II and Dodd-Frank.
        """
    )
 
    # ─────────────────────────────────────────────
    # STAGE 3 — SAVINGS CALCULATION
    # ─────────────────────────────────────────────
 
    @task(queue="dbt")
    def calculate_batch_savings(optimization: dict) -> dict:
        """
        Calculates the dollar value of savings from batching at
        current rates versus executing individually at reference rates.
        Routed to dedicated worker queue for computation-heavy workload.
        """
        daily_volume_usd = 1_250_000_000
        favorable_currencies = optimization["favorable_currencies"]
        analysis = optimization["analysis"]
 
        total_bps_improvement = 0
        savings_by_currency = {}
 
        for currency in favorable_currencies:
            bps = analysis[currency]["bps_vs_reference"]
            currency_volume = daily_volume_usd / len(analysis)
            savings = currency_volume * (bps / 10000)
            savings_by_currency[currency] = round(savings, 2)
            total_bps_improvement += bps
            print(f"{currency}: {bps} bps improvement = ${savings:,.2f} potential savings")
 
        total_savings = sum(savings_by_currency.values())
        avg_bps = round(total_bps_improvement / len(favorable_currencies), 2) if favorable_currencies else 0
 
        print(f"Total potential batch savings: ${total_savings:,.2f}")
        print(f"Average improvement: {avg_bps} bps")
 
        return {
            "total_potential_savings_usd": round(total_savings, 2),
            "avg_bps_improvement": avg_bps,
            "savings_by_currency": savings_by_currency,
            "favorable_currency_count": len(favorable_currencies)
        }
 
    # ─────────────────────────────────────────────
    # STAGE 4 — DECISION
    # ─────────────────────────────────────────────
 
    @task
    def generate_treasury_recommendation(
        optimization: dict,
        volatility: dict,
        savings: dict
    ) -> dict:
        """
        Generates the treasury recommendation from three inputs:
        rate favorability, volatility assessment, and savings calculation.
        Every assumption is documented and auditable. This replaces
        a trader's judgment call with a reproducible decision process.
        """
        favorable_count = optimization["favorable_count"]
        volatility_detected = volatility["volatility_detected"]
        total_savings = savings["total_potential_savings_usd"]
        avg_bps = savings["avg_bps_improvement"]
 
        if volatility_detected:
            recommendation = "HOLD"
            rationale = f"Market volatility detected in {volatility['volatile_currencies']}. Delay batching until conditions stabilize."
        elif favorable_count >= 4 and total_savings > 100000:
            recommendation = "BATCH NOW"
            rationale = f"{favorable_count} currencies favorable. Estimated savings ${total_savings:,.2f} at {avg_bps} bps improvement."
        elif favorable_count >= 2:
            recommendation = "PARTIAL BATCH"
            rationale = f"Batch favorable currencies only: {optimization['favorable_currencies']}. Hold others."
        else:
            recommendation = "HOLD"
            rationale = "Insufficient rate improvement to justify batching at this time."
 
        print(f"TREASURY RECOMMENDATION: {recommendation}")
        print(f"Rationale: {rationale}")
        print(f"Market condition: {volatility['market_condition']}")
 
        return {
            "recommendation": recommendation,
            "rationale": rationale,
            "favorable_currencies": optimization["favorable_currencies"],
            "estimated_savings": total_savings,
            "avg_bps_improvement": avg_bps,
            "market_condition": volatility["market_condition"]
        }
 
    # ─────────────────────────────────────────────
    # STAGE 5 — ACTION
    # Downstream system triggers based on recommendation
    # ─────────────────────────────────────────────
 
    # Empty operator — treasury desk notification
    notify_treasury_desk = EmptyOperator(
        task_id="notify_treasury_desk",
        doc_md="""
        **Production**: Pushes the recommendation to the treasury desk's
        workflow system via internal messaging API. The trader sees the
        recommendation, the rationale, the favorable currencies, and the
        estimated savings — all pre-calculated. Decision time drops from
        45 minutes of manual rate checking to a one-click confirmation.
        """
    )
 
    # Empty operator — batch execution trigger
    trigger_batch_execution = EmptyOperator(
        task_id="trigger_batch_execution",
        doc_md="""
        **Production**: When recommendation is BATCH NOW, initiates the
        actual batch execution in the payment rails system via secure
        API call. Locks in the favorable rates identified by the pipeline.
        Zero manual intervention. 90 seconds from rate pull to execution.
        """
    )
 
    # Empty operator — risk committee escalation
    escalate_to_risk_committee = EmptyOperator(
        task_id="escalate_to_risk_committee",
        doc_md="""
        **Production**: When recommendation is HOLD due to volatility,
        routes an escalation to the risk committee with full context —
        which currencies triggered the alert, current vs threshold
        movements, and recommended monitoring interval. Replaces
        ad-hoc phone calls with a structured, documented escalation.
        """
    )
 
    # ─────────────────────────────────────────────
    # STAGE 6 — AUDIT AND COMPLIANCE
    # ─────────────────────────────────────────────
 
    @task
    def log_audit_record(
        rates: dict,
        recommendation: dict,
        savings: dict
    ) -> None:
        """
        Writes the complete decision record to Snowflake.
        Every run produces an immutable compliance record:
        timestamp, rates analyzed, recommendation, rationale,
        estimated savings. Regulators can audit every decision.
        This is not just operationally useful — it is a
        regulatory requirement under Basel III and Dodd-Frank.
        """
        import uuid
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
 
        run_id = str(uuid.uuid4())[:8]
 
        print("=" * 60)
        print("FX SETTLEMENT OPTIMIZER — AUDIT LOG")
        print("=" * 60)
        print(f"Run ID: {run_id}")
        print(f"Timestamp: {rates['timestamp']}")
        print(f"Base Currency: {rates['base']}")
        print(f"Market Condition: {recommendation['market_condition']}")
        print(f"Recommendation: {recommendation['recommendation']}")
        print(f"Rationale: {recommendation['rationale']}")
        print(f"Estimated Savings: ${savings['total_potential_savings_usd']:,.2f}")
        print("Writing to Snowflake compliance table...")
 
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
 
        sql = """
        INSERT INTO BANKING_DEMO.FX_OPERATIONS.FX_AUDIT_LOG (
            RUN_ID, TIMESTAMP, BASE_CURRENCY, RECOMMENDATION,
            RATIONALE, FAVORABLE_CURRENCIES, ESTIMATED_SAVINGS,
            AVG_BPS_IMPROVEMENT, MARKET_CONDITION
        ) VALUES (
            %(run_id)s, %(timestamp)s, %(base_currency)s,
            %(recommendation)s, %(rationale)s, %(favorable_currencies)s,
            %(estimated_savings)s, %(avg_bps)s, %(market_condition)s
        )
        """
 
        hook.run(sql, parameters={
            "run_id": run_id,
            "timestamp": rates["timestamp"],
            "base_currency": rates["base"],
            "recommendation": recommendation["recommendation"],
            "rationale": recommendation["rationale"],
            "favorable_currencies": str(recommendation["favorable_currencies"]),
            "estimated_savings": savings["total_potential_savings_usd"],
            "avg_bps": savings.get("avg_bps_improvement", 0),
            "market_condition": recommendation["market_condition"]
        })
 
        print(f"Audit record {run_id} written to BANKING_DEMO.FX_OPERATIONS.FX_AUDIT_LOG")
        print("=" * 60)
 
    # Empty operator — compliance data lake archival
    archive_decision_record = EmptyOperator(
        task_id="archive_decision_record",
        doc_md="""
        **Production**: Archives the complete decision record — rates,
        analysis, recommendation, action taken — to the compliance
        data lake for long-term retention. Satisfies 7-year regulatory
        record-keeping requirements under OCC and Federal Reserve guidance.
        Downstream lineage tracked via OpenLineage for full auditability.
        """
    )
 
    # ─────────────────────────────────────────────
    # WIRE UP THE PIPELINE
    # ─────────────────────────────────────────────
 
    # Stage 1 — Ingest
    rates = pull_live_fx_rates()
    load_historical_rates.set_upstream(rates)

    # Stage 2 — Parallel analysis
    optimization = identify_optimal_batch_currencies(rates)
    run_arbitrage_model.set_upstream(optimization)
    volatility = detect_rate_volatility(rates)
    check_peer_bank_rates.set_upstream(volatility)

    # Stage 3 — Savings
    savings = calculate_batch_savings(optimization)

    # Stage 4 — Decision
    recommendation = generate_treasury_recommendation(optimization, volatility, savings)

    # Stage 5 — Action
    notify_treasury_desk.set_upstream(recommendation)
    trigger_batch_execution.set_upstream(notify_treasury_desk)
    escalate_to_risk_committee.set_upstream(recommendation)

    # Stage 6 — Audit
    audit = log_audit_record(rates, recommendation, savings)
    audit.set_upstream(trigger_batch_execution)
    audit.set_upstream(escalate_to_risk_committee)
    archive_decision_record.set_upstream(audit)
 
fx_settlement_optimizer()
 