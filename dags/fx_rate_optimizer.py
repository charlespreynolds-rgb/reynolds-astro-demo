from airflow.sdk import dag, task
from pendulum import datetime
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(
    start_date=datetime(2025, 1, 1),
    # schedule="0 */4 * * *",  # Re-enable for production - runs every 4 hours
    schedule=None
    default_args={"owner": "charles", "retries": 2},
    tags=["banking", "fx", "treasury"],
)
def fx_rate_optimizer():

    @task
    def pull_live_fx_rates() -> dict:
        try:
            response = requests.get("https://open.er-api.com/v6/latest/USD")
            response.raise_for_status()
            data = response.json()
            rates = data["rates"]
            print(f"Successfully pulled live FX rates. Base currency: USD")
            print(f"Rate update time: {data['time_last_update_utc']}")
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
            print(f"API unavailable, using reference rates: {e}")
            return {
                "base": "USD",
                "timestamp": "2025-01-15T06:00:00Z",
                "rates": {
                    "EUR": 0.9187,
                    "GBP": 0.7923,
                    "JPY": 149.82,
                    "CAD": 1.3641,
                    "AUD": 1.5712,
                    "CHF": 0.8934,
                    "CNY": 7.2341,
                    "MXN": 17.1523,
                }
            }

    @task
    def identify_optimal_batch_currencies(rate_data: dict) -> dict:
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
        print(f"Found {len(favorable)} currencies with favorable rates for batching")

        return {
            "timestamp": rate_data["timestamp"],
            "analysis": improvements,
            "favorable_currencies": list(favorable.keys()),
            "favorable_count": len(favorable)
        }

    @task
    def detect_rate_volatility(rate_data: dict) -> dict:
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
                print(f"{currency}: Movement {movement:.4f} within normal range")

        volatility_detected = len(volatile_currencies) > 0

        return {
            "volatility_detected": volatility_detected,
            "volatile_currencies": volatile_currencies,
            "market_condition": "VOLATILE" if volatility_detected else "STABLE"
        }

    @task(queue="dbt")
    def calculate_batch_savings(optimization: dict) -> dict:
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

        return {
            "total_potential_savings_usd": round(total_savings, 2),
            "avg_bps_improvement": avg_bps,
            "savings_by_currency": savings_by_currency,
            "favorable_currency_count": len(favorable_currencies)
        }

    @task
    def generate_treasury_recommendation(
        optimization: dict,
        volatility: dict,
        savings: dict
    ) -> dict:
        favorable_count = optimization["favorable_count"]
        volatility_detected = volatility["volatility_detected"]
        total_savings = savings["total_potential_savings_usd"]
        avg_bps = savings["avg_bps_improvement"]

        if volatility_detected:
            recommendation = "HOLD"
            rationale = f"Market volatility detected in {volatility['volatile_currencies']}. Delay batching until conditions stabilize."
        elif favorable_count >= 4 and total_savings > 100000:
            recommendation = "BATCH NOW"
            rationale = f"{favorable_count} currencies showing favorable rates. Estimated savings of ${total_savings:,.2f} at {avg_bps} bps improvement."
        elif favorable_count >= 2:
            recommendation = "PARTIAL BATCH"
            rationale = f"Batch favorable currencies only: {optimization['favorable_currencies']}. Hold others."
        else:
            recommendation = "HOLD"
            rationale = "Insufficient rate improvement to justify batching at this time."

        print(f"TREASURY RECOMMENDATION: {recommendation}")
        print(f"Rationale: {rationale}")

        return {
            "recommendation": recommendation,
            "rationale": rationale,
            "favorable_currencies": optimization["favorable_currencies"],
            "estimated_savings": total_savings,
            "avg_bps_improvement": avg_bps,
            "market_condition": volatility["market_condition"]
        }

    @task
    def log_audit_record(
        rates: dict,
        recommendation: dict,
        savings: dict
    ) -> None:
        import uuid

        run_id = str(uuid.uuid4())[:8]

        print("=" * 60)
        print("FX BATCH OPTIMIZATION AUDIT LOG")
        print("=" * 60)
        print(f"Timestamp: {rates['timestamp']}")
        print(f"Base Currency: {rates['base']}")
        print(f"Market Condition: {recommendation['market_condition']}")
        print(f"Recommendation: {recommendation['recommendation']}")
        print(f"Estimated Savings: ${savings['total_potential_savings_usd']:,.2f}")
        print("Writing audit record to Snowflake...")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        sql = """
        INSERT INTO BANKING_DEMO.FX_OPERATIONS.FX_AUDIT_LOG (
            RUN_ID,
            TIMESTAMP,
            BASE_CURRENCY,
            RECOMMENDATION,
            RATIONALE,
            FAVORABLE_CURRENCIES,
            ESTIMATED_SAVINGS,
            AVG_BPS_IMPROVEMENT,
            MARKET_CONDITION
        ) VALUES (
            %(run_id)s,
            %(timestamp)s,
            %(base_currency)s,
            %(recommendation)s,
            %(rationale)s,
            %(favorable_currencies)s,
            %(estimated_savings)s,
            %(avg_bps)s,
            %(market_condition)s
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

        print(f"Audit record {run_id} successfully written to Snowflake")
        print(f"Table: BANKING_DEMO.FX_OPERATIONS.FX_AUDIT_LOG")
        print("=" * 60)

    rates = pull_live_fx_rates()
    optimization = identify_optimal_batch_currencies(rates)
    volatility = detect_rate_volatility(rates)
    savings = calculate_batch_savings(optimization)
    recommendation = generate_treasury_recommendation(optimization, volatility, savings)
    log_audit_record(rates, recommendation, savings)

fx_rate_optimizer()
