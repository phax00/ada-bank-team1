from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd


SEED = 42
RNG = np.random.default_rng(SEED)
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "bronze_data"
DEMO_DIR = DATA_DIR / "demo"
SNAPSHOT_DATE = pd.Timestamp("2025-12-31")
HISTORY_START = pd.Timestamp("2025-10-01")
PLAN_YEAR = 2026
TEAM_ID = "team1-digi"

CURRENT_CLIENTS = 10_000
CURRENT_INSURED_CLIENTS = 2_000
CURRENT_LOAN_CLIENTS = 1_000
CURRENT_LOAN_VOLUME_CZK = 320_000_000

TARGET_NEW_CLIENTS = 50_000
TARGET_DIGITAL_ONBOARDINGS = 25_000
TARGET_ASSISTED_ONBOARDINGS = 25_000
TARGET_INSURED_CLIENTS_TOTAL = 15_000
TARGET_NEW_INSURED_CLIENTS = TARGET_INSURED_CLIENTS_TOTAL - CURRENT_INSURED_CLIENTS
TARGET_LOAN_VOLUME_CZK = 4_000_000_000
REQUIRED_AVG_TICKET_WITHOUT_MORTGAGE = 1_510_400

TRAFFIC_DATES = pd.date_range(f"{PLAN_YEAR}-01-01", f"{PLAN_YEAR}-12-31", freq="D")
WEB_USERS = 185_000
TARGET_APPLICATIONS = 56_000
AVG_DAILY_BUDGET = 185_000

CAMPAIGNS = [
    ("GOO_BRAND_ONB", "Google Brand Onboarding", "google", "google", "cpc", "/konto/zalozeni", 0.23, 1.10),
    ("GOO_GENERIC_ONB", "Google Generic Onboarding", "google", "google", "cpc", "/konto/online", 0.21, 0.86),
    ("META_APP_INSTALL", "Meta App Install", "meta", "meta", "paid_social", "/app/onboarding", 0.18, 0.73),
    ("META_RETARGETING", "Meta Retargeting", "meta", "meta", "paid_social", "/konto/dokonceni", 0.12, 1.18),
    ("AFFILIATE_COMPARATORS", "Affiliate Comparators", "affiliate", "affiliate", "cpa", "/konto/srovnani", 0.17, 1.00),
    ("DISPLAY_PROSPECTING", "Display Prospecting", "display", "programmatic", "display", "/konto/benefity", 0.09, 0.52),
]


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEMO_DIR.mkdir(parents=True, exist_ok=True)


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def seasonal(day: pd.Timestamp) -> float:
    month = {1: 1.04, 2: 0.98, 3: 1.01, 4: 0.97, 5: 0.99, 6: 0.95, 7: 0.90, 8: 0.88, 9: 1.05, 10: 1.08, 11: 1.16, 12: 1.22}[day.month]
    weekday = 1.06 if day.dayofweek <= 2 else 0.96
    return month * weekday


def build_ads_plan() -> pd.DataFrame:
    rows = []
    for day in TRAFFIC_DATES:
        daily_budget = AVG_DAILY_BUDGET * seasonal(day)
        for i, c in enumerate(CAMPAIGNS, start=1):
            campaign_id, campaign_name, platform, source, medium, page, share, intent = c
            spend = daily_budget * share * RNG.uniform(0.92, 1.08)
            cpc = {"google": RNG.uniform(22, 34), "meta": RNG.uniform(18, 29), "affiliate": RNG.uniform(45, 70), "display": RNG.uniform(11, 18)}[platform]
            ctr = {"google": RNG.uniform(0.032, 0.058), "meta": RNG.uniform(0.011, 0.023), "affiliate": RNG.uniform(0.020, 0.038), "display": RNG.uniform(0.006, 0.012)}[platform]
            clicks = max(50, int(round(spend / cpc)))
            impressions = max(clicks, int(round(clicks / ctr)))
            rows.append(
                {
                    "date": day.date(),
                    "platform": platform,
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "adset_id": f"{platform}_adset_{i}",
                    "cost_czk": round(spend, 2),
                    "clicks": clicks,
                    "impressions": impressions,
                    "currency": "CZK",
                    "dq_flag": "PASS",
                    "source": source,
                    "medium": medium,
                    "landing_page": page,
                    "intent": intent,
                    "weight": clicks * intent,
                }
            )
    return pd.DataFrame(rows)


def build_plan_web_events(ads_plan: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    idx = RNG.choice(ads_plan.index.to_numpy(), size=WEB_USERS, p=ads_plan["weight"].to_numpy() / ads_plan["weight"].sum())
    users = ads_plan.loc[idx].reset_index(drop=True)
    users["user_id"] = [f"user_{i:06d}" for i in range(1, WEB_USERS + 1)]
    users["first_seen"] = pd.to_datetime(users["date"]) + pd.to_timedelta(RNG.integers(0, 24 * 60, len(users)), unit="m")
    users["device_type"] = np.where(RNG.random(len(users)) < 0.78, "mobile", "desktop")
    users["region"] = "CZ"
    users["return_sessions"] = np.clip(RNG.poisson(0.45, len(users)), 0, 3)
    users["session_count"] = 1 + users["return_sessions"]

    month_lift = users["first_seen"].dt.month.map({11: 0.25, 12: 0.18, 9: 0.11, 1: 0.09}).fillna(0.0)
    score = (
        0.9
        + month_lift
        + (users["intent"] - 1.0)
        + np.where(users["device_type"].eq("mobile"), 0.18, -0.08)
        + users["return_sessions"] * 0.22
        + RNG.normal(0, 0.42, len(users))
    )
    users["application_score"] = score
    users["is_application"] = users["application_score"].rank(method="first", ascending=False) <= TARGET_APPLICATIONS
    act_score = users["application_score"] + 0.35 * users["device_type"].eq("mobile") + RNG.normal(0, 0.25, len(users))
    activated = set(
        users.loc[users["is_application"]]
        .assign(activation_score=act_score[users["is_application"].to_numpy()])
        .sort_values("activation_score", ascending=False)
        .head(TARGET_DIGITAL_ONBOARDINGS)["user_id"]
    )
    users["is_activated"] = users["user_id"].isin(activated)

    sessions, conversions, identity, plan_clients, plan_accounts = [], [], [], [], []
    for row in users.itertuples(index=False):
        session_ids = []
        for offset in range(int(row.session_count)):
            sid = str(uuid.uuid4())
            session_ids.append(sid)
            when = pd.Timestamp(row.first_seen) + pd.Timedelta(hours=6 * offset) + pd.Timedelta(minutes=int(RNG.integers(0, 90)))
            sessions.append(
                {
                    "session_id": sid,
                    "user_id": row.user_id,
                    "event_time": when,
                    "source": row.source,
                    "medium": row.medium,
                    "campaign": row.campaign_id,
                    "gclid": stable_hash(f"gclid-{sid}")[:16] if row.platform == "google" else None,
                    "fbclid": stable_hash(f"fbclid-{sid}")[:16] if row.platform == "meta" else None,
                    "landing_page": row.landing_page,
                    "device_type": row.device_type,
                    "region": row.region,
                    "dq_flag": "FAIL" if RNG.random() < 0.0025 else "PASS",
                }
            )
        if row.is_application:
            conversion_time = pd.Timestamp(row.first_seen) + pd.Timedelta(hours=12) + pd.Timedelta(minutes=int(RNG.integers(20, 240)))
            conversions.append(
                {
                    "conversion_id": str(uuid.uuid4()),
                    "user_id": row.user_id,
                    "session_id": session_ids[-1],
                    "event_time": conversion_time,
                    "conversion_type": "account_application",
                    "form_id": "digital_current_account",
                    "dq_flag": "PASS",
                }
            )
        if row.is_activated:
            client_id = f"PLAN_D_{len(identity) + 1:05d}"
            created_at = conversion_time if row.is_application else pd.Timestamp(row.first_seen) + pd.Timedelta(hours=14)
            activated_at = created_at + pd.Timedelta(hours=int(RNG.integers(1, 72)))
            identity.append(
                {
                    "user_id": row.user_id,
                    "hashed_email": stable_hash(f"{row.user_id}@nova-banka.demo"),
                    "client_id": client_id,
                    "dq_flag": "PASS",
                }
            )
            app_user = bool(RNG.random() < (0.94 if row.device_type == "mobile" else 0.74))
            plan_clients.append(
                {
                    "client_id": client_id,
                    "created_at": created_at,
                    "status": "active",
                    "acquisition_channel": "digital",
                    "onboarding_mode": "pure_digital",
                    "mobile_app_user": app_user,
                    "hashed_email": stable_hash(f"{row.user_id}@nova-banka.demo"),
                    "dq_flag": "PASS",
                }
            )
            plan_accounts.append(
                {
                    "account_id": f"ACC_P_{len(plan_accounts) + 1:05d}",
                    "client_id": client_id,
                    "created_at": created_at,
                    "activated_at": activated_at,
                    "status": "active",
                    "product_type": "current_account",
                    "dq_flag": "PASS",
                }
            )
    return (
        pd.DataFrame(sessions),
        pd.DataFrame(conversions),
        pd.DataFrame(identity),
        pd.DataFrame(plan_clients),
        pd.DataFrame(plan_accounts),
    )


def build_current_base_clients() -> tuple[pd.DataFrame, pd.DataFrame]:
    client_ids = [f"CLI_E_{i:05d}" for i in range(1, CURRENT_CLIENTS + 1)]
    created = pd.to_datetime(RNG.choice(pd.date_range("2023-01-01", "2024-12-31", freq="D"), CURRENT_CLIENTS)) + pd.to_timedelta(RNG.integers(7, 19, CURRENT_CLIENTS), unit="h")
    acquisition_channel = np.array(["digital"] * 3000 + ["branch"] * 5500 + ["call_center"] * 1500, dtype=object)
    RNG.shuffle(acquisition_channel)
    age_bands = ["18-24", "25-34", "35-44", "45-54", "55+"]
    age_weights = [0.28, 0.31, 0.19, 0.13, 0.09]
    internal_age_band = RNG.choice(age_bands, size=CURRENT_CLIENTS, p=age_weights)
    clients = pd.DataFrame(
        {
            "client_id": client_ids,
            "created_at": created,
            "status": "active",
            "acquisition_channel": acquisition_channel,
            "onboarding_mode": np.where(acquisition_channel == "digital", "pure_digital", "assisted"),
            "hashed_email": [stable_hash(f"{cid}@nova-banka.demo") for cid in client_ids],
            "internal_age_band": internal_age_band,
            "dq_flag": "PASS",
        }
    )
    app_adoption = (
        clients["internal_age_band"].map({"18-24": 0.82, "25-34": 0.79, "35-44": 0.68, "45-54": 0.56, "55+": 0.44}).astype(float)
        + 0.05 * clients["onboarding_mode"].eq("pure_digital").astype(float)
        - 0.03 * clients["acquisition_channel"].eq("call_center").astype(float)
    ).clip(0.25, 0.93)
    clients["mobile_app_user"] = RNG.random(CURRENT_CLIENTS) < app_adoption
    clients = clients.sort_values(["created_at", "client_id"]).reset_index(drop=True)
    accounts = pd.DataFrame(
        {
            "account_id": [f"ACC_E_{i:05d}" for i in range(1, CURRENT_CLIENTS + 1)],
            "client_id": client_ids,
            "created_at": created,
            "activated_at": created + pd.to_timedelta(RNG.integers(1, 72, CURRENT_CLIENTS), unit="h"),
            "status": "active",
            "product_type": "current_account",
            "dq_flag": "PASS",
        }
    ).sort_values(["activated_at", "client_id"]).reset_index(drop=True)
    return clients, accounts


def build_current_transactions(current_clients: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    clients = current_clients[["client_id", "internal_age_band", "acquisition_channel", "onboarding_mode", "mobile_app_user", "created_at"]].copy()
    age_score = clients["internal_age_band"].map({"18-24": -0.18, "25-34": 0.10, "35-44": 0.12, "45-54": 0.06, "55+": -0.05})
    vehicle_base = (
        -0.02
        + age_score
        + 0.05 * clients["mobile_app_user"].astype(int)
        + 0.04 * clients["acquisition_channel"].eq("digital").astype(int)
        + RNG.normal(0, 0.12, len(clients))
    )
    clients["owns_vehicle"] = sigmoid(vehicle_base) > 0.54

    insured_ids = set(RNG.choice(clients["client_id"].to_numpy(), size=CURRENT_INSURED_CLIENTS, replace=False).tolist())
    vehicle_insured_ids = set(RNG.choice(list(insured_ids), size=900, replace=False).tolist())
    clients["has_any_insurance"] = clients["client_id"].isin(insured_ids)
    clients["has_vehicle_insurance"] = clients["client_id"].isin(vehicle_insured_ids)

    need = (
        0.10
        + 0.95 * clients["owns_vehicle"].astype(int)
        + 0.25 * clients["internal_age_band"].isin(["25-34", "35-44"]).astype(int)
        + 0.15 * clients["acquisition_channel"].eq("digital").astype(int)
        + 0.08 * clients["mobile_app_user"].astype(int)
        + RNG.normal(0, 0.25, len(clients))
    )
    clients["latent_vehicle_need"] = sigmoid(need)
    clients["target_buy_vehicle_insurance_30d"] = (
        clients["owns_vehicle"]
        & ~clients["has_vehicle_insurance"]
        & (clients["latent_vehicle_need"] > 0.72)
        & (RNG.random(len(clients)) < np.clip(clients["latent_vehicle_need"] * 0.18, 0.01, 0.16))
    )

    transactions = []

    def add_tx(cid: str, when: pd.Timestamp, amount: float, mcc: int, merchant: str) -> None:
        transactions.append(
            {
                "transaction_id": str(uuid.uuid4()),
                "client_id": cid,
                "date": when,
                "amount": round(float(amount), 2),
                "mcc_code": mcc,
                "merchant": merchant,
                "dq_flag": "FAIL" if RNG.random() < 0.001 else "PASS",
            }
        )

    for row in clients.itertuples(index=False):
        active_days = max(14, min((SNAPSHOT_DATE - HISTORY_START).days + 1, int((SNAPSHOT_DATE - pd.Timestamp(row.created_at)).days)))
        general = max(10, int(RNG.poisson(12 + 3 * (row.internal_age_band in ["18-24", "25-34"]) + 2 * row.mobile_app_user)))
        for _ in range(general):
            offset = int(RNG.integers(0, active_days))
            when = SNAPSHOT_DATE - pd.Timedelta(days=offset) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
            scenarios = [
                ("grocery_market", 5411, 120, 1800),
                ("restaurant", 5812, 160, 1400),
                ("ecommerce", 5942, 200, 3200),
                ("utilities", 4900, 500, 4200),
                ("travel", 4722, 450, 6500),
            ]
            merchant, mcc, low, high = scenarios[int(RNG.choice(np.arange(len(scenarios)), p=[0.32, 0.18, 0.21, 0.17, 0.12]))]
            add_tx(row.client_id, when, RNG.uniform(low, high), int(mcc), merchant)

        if row.owns_vehicle:
            for _ in range(max(0, int(RNG.poisson(1.8 + 2.6 * row.latent_vehicle_need)))):
                when = SNAPSHOT_DATE - pd.Timedelta(days=int(RNG.integers(0, min(active_days, 90)))) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
                add_tx(row.client_id, when, RNG.uniform(900, 2600), int(RNG.choice([5541, 5542])), "fuel_station")
            for _ in range(int(RNG.binomial(2, min(0.12 + 0.22 * row.latent_vehicle_need, 0.55)))):
                when = SNAPSHOT_DATE - pd.Timedelta(days=int(RNG.integers(10, min(active_days, 90)))) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
                add_tx(row.client_id, when, RNG.uniform(1800, 9200), 7538, "car_service")
            if RNG.random() < min(0.06 + 0.14 * row.latent_vehicle_need, 0.28):
                when = SNAPSHOT_DATE - pd.Timedelta(days=int(RNG.integers(0, min(active_days, 90)))) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
                add_tx(row.client_id, when, RNG.uniform(900, 2100), 4784, "dalnicni_znamka")

        if row.has_vehicle_insurance:
            for _ in range(int(RNG.integers(1, 3))):
                when = SNAPSHOT_DATE - pd.Timedelta(days=int(RNG.integers(20, min(active_days + 1, 120)))) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
                add_tx(row.client_id, when, RNG.uniform(2800, 7600), 6300, "insurance_company")

        if row.target_buy_vehicle_insurance_30d:
            when = SNAPSHOT_DATE + pd.Timedelta(days=int(RNG.integers(2, 30))) + pd.Timedelta(minutes=int(RNG.integers(0, 1440)))
            add_tx(row.client_id, when, RNG.uniform(3200, 8200), 6300, "insurance_company")

    labels = clients[["client_id", "target_buy_vehicle_insurance_30d"]].copy()
    labels["has_any_insurance"] = clients["has_any_insurance"].astype(int)
    labels["has_vehicle_insurance"] = clients["has_vehicle_insurance"].astype(int)
    return pd.DataFrame(transactions).sort_values(["date", "client_id"]).reset_index(drop=True), labels


def build_vehicle_frame(current_clients: pd.DataFrame, transactions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    hist = transactions.loc[(transactions["date"] >= HISTORY_START) & (transactions["date"] <= SNAPSHOT_DATE) & (transactions["dq_flag"] == "PASS")].copy()
    hist["days_from_snapshot"] = (SNAPSHOT_DATE - hist["date"]).dt.days
    hist["fuel_flag"] = hist["mcc_code"].isin([5541, 5542]).astype(int)
    hist["service_flag"] = hist["mcc_code"].eq(7538).astype(int)
    hist["insurance_flag"] = hist["merchant"].eq("insurance_company").astype(int)
    hist["vignette_flag"] = hist["merchant"].eq("dalnicni_znamka").astype(int)

    base = current_clients[["client_id", "internal_age_band", "mobile_app_user", "onboarding_mode"]].drop_duplicates()
    last_ins = hist.loc[hist["insurance_flag"].eq(1)].groupby("client_id")["date"].max().rename("last_insurance_date")
    p30 = hist.loc[hist["days_from_snapshot"] <= 30].groupby("client_id").agg(
        fuel_spend_30d=("amount", lambda s: float(s[hist.loc[s.index, "fuel_flag"].eq(1)].sum())),
        fuel_txn_count_30d=("fuel_flag", "sum"),
    )
    p90 = hist.loc[hist["days_from_snapshot"] <= 90].groupby("client_id").agg(
        fuel_spend_90d=("amount", lambda s: float(s[hist.loc[s.index, "fuel_flag"].eq(1)].sum())),
        service_txn_count_90d=("service_flag", "sum"),
        total_txn_count_90d=("transaction_id", "count"),
        avg_txn_amount=("amount", "mean"),
        highway_vignette_flag=("vignette_flag", "max"),
        insurance_payment_flag=("insurance_flag", "max"),
    )

    frame = (
        base.merge(p30, on="client_id", how="left")
        .merge(p90, on="client_id", how="left")
        .merge(last_ins, on="client_id", how="left")
        .merge(labels, on=["client_id"], how="left")
    )
    frame["region"] = "CZ"
    frame["age_band"] = frame["internal_age_band"]
    for col in ["fuel_spend_30d", "fuel_txn_count_30d", "fuel_spend_90d", "service_txn_count_90d", "total_txn_count_90d", "avg_txn_amount", "highway_vignette_flag", "insurance_payment_flag"]:
        frame[col] = frame[col].fillna(0)
    frame["days_since_last_insurance"] = (SNAPSHOT_DATE - frame["last_insurance_date"]).dt.days.fillna(9999).astype(int)
    frame["snapshot_date"] = SNAPSHOT_DATE.date()
    frame["business_date"] = SNAPSHOT_DATE.date()
    frame["dq_flag"] = "PASS"
    frame["target_buy_vehicle_insurance_30d"] = frame["target_buy_vehicle_insurance_30d"].fillna(0).astype(int)
    return frame.drop(columns=["last_insurance_date", "internal_age_band"])


def run_propensity_demo(frame: pd.DataFrame):
    cols = ["fuel_spend_30d", "fuel_txn_count_30d", "fuel_spend_90d", "service_txn_count_90d", "highway_vignette_flag", "insurance_payment_flag", "days_since_last_insurance", "avg_txn_amount", "total_txn_count_90d"]
    demo = frame.loc[
        frame["dq_flag"].eq("PASS"),
        ["client_id", "region", "age_band", "mobile_app_user", "onboarding_mode", "target_buy_vehicle_insurance_30d"] + cols,
    ].copy()
    x = demo[cols].astype(float).copy()
    x["days_since_last_insurance"] = np.where(x["days_since_last_insurance"] > 365, 365, x["days_since_last_insurance"])
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import average_precision_score, roc_auc_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        pipe = Pipeline(
            steps=[
                ("prep", ColumnTransformer([("num", Pipeline([("imp", SimpleImputer(strategy="constant", fill_value=0.0)), ("sc", StandardScaler())]), cols)])),
                ("model", LogisticRegression(max_iter=250, class_weight="balanced", random_state=SEED)),
            ]
        )
        pipe.fit(x, demo["target_buy_vehicle_insurance_30d"])
        demo["propensity_score"] = pipe.predict_proba(x)[:, 1]
        metrics = pd.DataFrame(
            [
                {"metric": "roc_auc", "value": round(float(roc_auc_score(demo["target_buy_vehicle_insurance_30d"], demo["propensity_score"])), 4), "model_version": "logreg_v1"},
                {"metric": "average_precision", "value": round(float(average_precision_score(demo["target_buy_vehicle_insurance_30d"], demo["propensity_score"])), 4), "model_version": "logreg_v1"},
            ]
        )
        model_version = "logreg_v1"
    except Exception:
        score = -4.0 + 0.00022 * x["fuel_spend_90d"] + 0.16 * x["fuel_txn_count_30d"] + 0.42 * x["service_txn_count_90d"] + 0.95 * x["highway_vignette_flag"] - 1.80 * x["insurance_payment_flag"] - 0.0004 * x["days_since_last_insurance"] + 0.00005 * x["avg_txn_amount"] + 0.025 * x["total_txn_count_90d"]
        demo["propensity_score"] = sigmoid(score)
        metrics = pd.DataFrame([{"metric": "positive_rate", "value": round(float(demo["target_buy_vehicle_insurance_30d"].mean()), 4), "model_version": "rule_fallback_v1"}])
        model_version = "rule_fallback_v1"

    demo["segment"] = pd.cut(demo["propensity_score"], bins=[-np.inf, 0.35, 0.62, np.inf], labels=["LOW", "MEDIUM", "HIGH"]).astype(str)
    demo["recommended_channel_primary"] = np.select(
        [
            demo["insurance_payment_flag"].eq(1),
            demo["mobile_app_user"].eq(1) & demo["segment"].eq("HIGH"),
            demo["segment"].eq("HIGH"),
            demo["mobile_app_user"].eq(1) & demo["segment"].eq("MEDIUM"),
            demo["segment"].eq("MEDIUM"),
        ],
        [None, "push", "email", "push", "email"],
        default="banner_in_ib",
    )
    demo["recommended_channel"] = np.select(
        [
            demo["insurance_payment_flag"].eq(1),
            demo["mobile_app_user"].eq(1) & demo["segment"].eq("HIGH"),
            demo["segment"].eq("HIGH"),
            demo["mobile_app_user"].eq(1) & demo["segment"].eq("MEDIUM"),
            demo["segment"].eq("MEDIUM"),
        ],
        [
            None,
            "push + banner_in_ib + email",
            "email + banner_in_ib",
            "push + banner_in_ib",
            "email + banner_in_ib",
        ],
        default="banner_in_ib",
    )
    demo["next_best_action"] = np.where(demo["insurance_payment_flag"].eq(1), "exclude_existing_policy", "contact_client")
    demo["model_version"] = model_version
    demo["score_date"] = SNAPSHOT_DATE.date()
    top = (
        demo.loc[
            demo["next_best_action"].eq("contact_client"),
            [
                "client_id",
                "region",
                "age_band",
                "mobile_app_user",
                "propensity_score",
                "segment",
                "recommended_channel_primary",
                "recommended_channel",
                "model_version",
                "score_date",
            ],
        ]
        .sort_values("propensity_score", ascending=False)
        .head(200)
        .reset_index(drop=True)
    )
    top["contact_priority"] = np.arange(1, len(top) + 1)
    seg = demo.groupby("segment").agg(clients=("client_id", "count"), avg_score=("propensity_score", "mean"), positive_rate=("target_buy_vehicle_insurance_30d", "mean")).reset_index()
    return demo, top, seg, metrics


def build_acquisition_plan_monthly(ads_plan: pd.DataFrame, sessions: pd.DataFrame, conversions: pd.DataFrame, identity: pd.DataFrame) -> pd.DataFrame:
    ads_plan["month"] = pd.to_datetime(ads_plan["date"]).dt.to_period("M").astype(str)
    sessions = sessions.assign(month=sessions["event_time"].dt.to_period("M").astype(str))
    conversions = conversions.assign(month=conversions["event_time"].dt.to_period("M").astype(str))
    identity["month"] = pd.period_range(f"{PLAN_YEAR}-01", f"{PLAN_YEAR}-12", freq="M").repeat([2080, 1710, 1820, 1900, 1950, 2020, 2060, 2140, 2320, 2440, 3070, 3490])[: len(identity)].astype(str)
    monthly = (
        ads_plan.groupby("month", as_index=False)["cost_czk"].sum()
        .merge(sessions.groupby("month", as_index=False)["session_id"].nunique().rename(columns={"session_id": "sessions"}), on="month")
        .merge(conversions.groupby("month", as_index=False)["user_id"].nunique().rename(columns={"user_id": "applications"}), on="month")
        .merge(identity.groupby("month", as_index=False)["client_id"].nunique().rename(columns={"client_id": "digital_onboardings"}), on="month")
    )
    assisted_distribution = np.array([1900, 1750, 1800, 1850, 1900, 1950, 2050, 2100, 2150, 2200, 2500, 2900])
    adjusted = np.floor(assisted_distribution / assisted_distribution.sum() * TARGET_ASSISTED_ONBOARDINGS).astype(int)
    adjusted[-1] += TARGET_ASSISTED_ONBOARDINGS - int(adjusted.sum())
    monthly["assisted_onboardings"] = adjusted
    monthly["new_clients_total"] = monthly["digital_onboardings"] + monthly["assisted_onboardings"]
    monthly["digital_share"] = monthly["digital_onboardings"] / monthly["new_clients_total"]
    monthly["cpa_czk"] = monthly["cost_czk"] / monthly["digital_onboardings"]
    monthly["business_view"] = "plan"
    return monthly


def build_insurance_plan() -> pd.DataFrame:
    rows = [
        {"portfolio": "existing_clients", "product": "loss_and_theft", "target_policies": 600, "share_of_portfolio_target": 0.60, "conversion_rate": 0.07, "clients_to_contact": 8571.43},
        {"portfolio": "existing_clients", "product": "vehicle", "target_policies": 250, "share_of_portfolio_target": 0.25, "conversion_rate": 0.04, "clients_to_contact": 6250.00},
        {"portfolio": "existing_clients", "product": "travel_long_term", "target_policies": 150, "share_of_portfolio_target": 0.15, "conversion_rate": 0.10, "clients_to_contact": 1500.00},
        {"portfolio": "new_clients", "product": "loss_and_theft", "target_policies": 9600, "share_of_portfolio_target": 0.80, "conversion_rate": 0.20, "clients_to_contact": 48000.00},
        {"portfolio": "new_clients", "product": "vehicle", "target_policies": 600, "share_of_portfolio_target": 0.05, "conversion_rate": 0.02, "clients_to_contact": 30000.00},
        {"portfolio": "new_clients", "product": "travel_long_term", "target_policies": 1800, "share_of_portfolio_target": 0.15, "conversion_rate": 0.07, "clients_to_contact": 25714.29},
    ]
    return pd.DataFrame(rows)


def build_lending_plan() -> pd.DataFrame:
    rows = [
        {"portfolio": "existing_clients", "product": "consumer_loan", "target_volume_czk": 201_600_000, "share_of_volume": 0.90, "share_of_clients": 0.40, "avg_ticket_czk": 720_000},
        {"portfolio": "existing_clients", "product": "credit_card", "target_volume_czk": 22_400_000, "share_of_volume": 0.10, "share_of_clients": 0.60, "avg_ticket_czk": 53_333.33},
        {"portfolio": "new_clients", "product": "consumer_loan", "target_volume_czk": 680_000_000, "share_of_volume": 0.85, "share_of_clients": 0.10, "avg_ticket_czk": 2_720_000},
        {"portfolio": "new_clients", "product": "credit_card", "target_volume_czk": 120_000_000, "share_of_volume": 0.15, "share_of_clients": 0.90, "avg_ticket_czk": 53_333.33},
    ]
    plan = pd.DataFrame(rows)
    plan["warning"] = np.where(
        plan["avg_ticket_czk"] > 1_000_000,
        "Without mortgages or secured lending this avg ticket is not realistic.",
        "",
    )
    return plan


def build_income_cost(ads_plan: pd.DataFrame, acquisition_plan: pd.DataFrame) -> pd.DataFrame:
    months = pd.period_range(f"{PLAN_YEAR}-01", f"{PLAN_YEAR}-12", freq="M")
    spend = ads_plan.groupby(pd.to_datetime(ads_plan["date"]).dt.to_period("M"))["cost_czk"].sum()
    rows = []
    cumulative_new_clients = 0
    current_base_monthly_income_per_client = 580
    new_client_monthly_income_per_client = 165
    insurance_income_per_policy_month = 95
    base_servicing_cost_per_client = 105
    new_client_servicing_cost_per_client = 48
    assisted_onboarding_unit_cost = 420
    digital_platform_unit_cost = 68
    fte_cost_base = 340_000
    external_services_cost_base = 120_000
    platform_support_cost_base = 95_000
    project_management_cost_base = 135_000
    for month in months:
        month_row = acquisition_plan.loc[acquisition_plan["month"] == str(month)].iloc[0]
        month_new = int(month_row["new_clients_total"])
        month_digital = int(month_row["digital_onboardings"])
        month_assisted = int(month_row["assisted_onboardings"])
        cumulative_new_clients += month_new
        insurance_ramp = cumulative_new_clients / TARGET_NEW_CLIENTS
        planned_insurance_clients = TARGET_NEW_INSURED_CLIENTS * insurance_ramp

        current_base_income_czk = CURRENT_CLIENTS * RNG.uniform(current_base_monthly_income_per_client - 25, current_base_monthly_income_per_client + 25)
        new_clients_income_czk = cumulative_new_clients * RNG.uniform(new_client_monthly_income_per_client - 20, new_client_monthly_income_per_client + 20)
        insurance_income_czk = planned_insurance_clients * RNG.uniform(insurance_income_per_policy_month - 10, insurance_income_per_policy_month + 10)
        income = current_base_income_czk + new_clients_income_czk + insurance_income_czk

        marketing_cost_czk = float(spend.get(month, 0.0))
        current_base_servicing_cost_czk = CURRENT_CLIENTS * RNG.uniform(base_servicing_cost_per_client - 10, base_servicing_cost_per_client + 10)
        new_clients_servicing_cost_czk = cumulative_new_clients * RNG.uniform(new_client_servicing_cost_per_client - 8, new_client_servicing_cost_per_client + 8)
        assisted_onboarding_cost_czk = month_assisted * RNG.uniform(assisted_onboarding_unit_cost - 35, assisted_onboarding_unit_cost + 35)
        digital_platform_cost_czk = month_digital * RNG.uniform(digital_platform_unit_cost - 10, digital_platform_unit_cost + 10)
        fte_cost_czk = RNG.uniform(fte_cost_base - 35_000, fte_cost_base + 45_000)
        external_services_cost_czk = RNG.uniform(external_services_cost_base - 25_000, external_services_cost_base + 40_000)
        platform_support_cost_czk = RNG.uniform(platform_support_cost_base - 20_000, platform_support_cost_base + 25_000)
        project_management_cost_czk = RNG.uniform(project_management_cost_base - 20_000, project_management_cost_base + 20_000)
        cost = (
            marketing_cost_czk
            + current_base_servicing_cost_czk
            + new_clients_servicing_cost_czk
            + assisted_onboarding_cost_czk
            + digital_platform_cost_czk
            + fte_cost_czk
            + external_services_cost_czk
            + platform_support_cost_czk
            + project_management_cost_czk
        )
        rows.append(
            {
                "team_id": TEAM_ID,
                "period": str(month),
                "income_czk": round(float(income), 2),
                "cost_czk": round(float(cost), 2),
                "current_base_income_czk": round(float(current_base_income_czk), 2),
                "new_clients_income_czk": round(float(new_clients_income_czk), 2),
                "insurance_income_czk": round(float(insurance_income_czk), 2),
                "marketing_cost_czk": round(float(marketing_cost_czk), 2),
                "current_base_servicing_cost_czk": round(float(current_base_servicing_cost_czk), 2),
                "new_clients_servicing_cost_czk": round(float(new_clients_servicing_cost_czk), 2),
                "assisted_onboarding_cost_czk": round(float(assisted_onboarding_cost_czk), 2),
                "digital_platform_cost_czk": round(float(digital_platform_cost_czk), 2),
                "fte_cost_czk": round(float(fte_cost_czk), 2),
                "external_services_cost_czk": round(float(external_services_cost_czk), 2),
                "platform_support_cost_czk": round(float(platform_support_cost_czk), 2),
                "project_management_cost_czk": round(float(project_management_cost_czk), 2),
            }
        )
    out = pd.DataFrame(rows)
    out["dq_flag"] = ["PASS"] * 10 + ["WARN"] + ["FAIL"]
    return out


def build_current_income_cost_2025() -> pd.DataFrame:
    months = pd.period_range("2025-01", "2025-12", freq="M")
    rows = []
    current_base_monthly_income_per_client = 560
    insurance_income_per_policy_month = 90
    base_servicing_cost_per_client = 102
    digital_platform_base_cost = 185_000
    fte_cost_base = 255_000
    external_services_cost_base = 85_000
    platform_support_cost_base = 70_000
    project_management_cost_base = 92_000
    current_vehicle_policy_clients = 900

    for month in months:
        seasonality = 1.0 + (0.04 if month.month in [11, 12] else 0.0) - (0.03 if month.month in [7, 8] else 0.0)
        current_base_income_czk = CURRENT_CLIENTS * RNG.uniform(current_base_monthly_income_per_client - 20, current_base_monthly_income_per_client + 20) * seasonality
        insurance_income_czk = current_vehicle_policy_clients * RNG.uniform(insurance_income_per_policy_month - 8, insurance_income_per_policy_month + 8)
        current_base_servicing_cost_czk = CURRENT_CLIENTS * RNG.uniform(base_servicing_cost_per_client - 8, base_servicing_cost_per_client + 8)
        digital_platform_cost_czk = RNG.uniform(digital_platform_base_cost - 25_000, digital_platform_base_cost + 25_000)
        fte_cost_czk = RNG.uniform(fte_cost_base - 25_000, fte_cost_base + 30_000)
        external_services_cost_czk = RNG.uniform(external_services_cost_base - 20_000, external_services_cost_base + 20_000)
        platform_support_cost_czk = RNG.uniform(platform_support_cost_base - 15_000, platform_support_cost_base + 20_000)
        project_management_cost_czk = RNG.uniform(project_management_cost_base - 15_000, project_management_cost_base + 15_000)
        marketing_cost_czk = RNG.uniform(120_000, 240_000)
        cost = (
            current_base_servicing_cost_czk
            + digital_platform_cost_czk
            + marketing_cost_czk
            + fte_cost_czk
            + external_services_cost_czk
            + platform_support_cost_czk
            + project_management_cost_czk
        )
        income = current_base_income_czk + insurance_income_czk

        rows.append(
            {
                "team_id": TEAM_ID,
                "period": str(month),
                "income_czk": round(float(income), 2),
                "cost_czk": round(float(cost), 2),
                "current_base_income_czk": round(float(current_base_income_czk), 2),
                "new_clients_income_czk": 0.0,
                "insurance_income_czk": round(float(insurance_income_czk), 2),
                "marketing_cost_czk": round(float(marketing_cost_czk), 2),
                "current_base_servicing_cost_czk": round(float(current_base_servicing_cost_czk), 2),
                "new_clients_servicing_cost_czk": 0.0,
                "assisted_onboarding_cost_czk": 0.0,
                "digital_platform_cost_czk": round(float(digital_platform_cost_czk), 2),
                "fte_cost_czk": round(float(fte_cost_czk), 2),
                "external_services_cost_czk": round(float(external_services_cost_czk), 2),
                "platform_support_cost_czk": round(float(platform_support_cost_czk), 2),
                "project_management_cost_czk": round(float(project_management_cost_czk), 2),
            }
        )

    out = pd.DataFrame(rows)
    out["dq_flag"] = ["PASS"] * 10 + ["WARN"] + ["FAIL"]
    return out


def build_product_cost_allocation(
    current_income_cost: pd.DataFrame, income_cost: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    products = [
        "strategy_marketing_performance_acquisition_aggregation",
        "retail_sales_vehicle_insurance_propensity_state",
    ]
    cost_columns = [
        "marketing_cost_czk",
        "current_base_servicing_cost_czk",
        "new_clients_servicing_cost_czk",
        "assisted_onboarding_cost_czk",
        "digital_platform_cost_czk",
        "fte_cost_czk",
        "external_services_cost_czk",
        "platform_support_cost_czk",
        "project_management_cost_czk",
    ]
    allocation_rules = {
        "marketing_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 1.00,
            "retail_sales_vehicle_insurance_propensity_state": 0.00,
        },
        "current_base_servicing_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.55,
            "retail_sales_vehicle_insurance_propensity_state": 0.45,
        },
        "new_clients_servicing_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.85,
            "retail_sales_vehicle_insurance_propensity_state": 0.15,
        },
        "assisted_onboarding_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 1.00,
            "retail_sales_vehicle_insurance_propensity_state": 0.00,
        },
        "digital_platform_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.70,
            "retail_sales_vehicle_insurance_propensity_state": 0.30,
        },
        "fte_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.45,
            "retail_sales_vehicle_insurance_propensity_state": 0.55,
        },
        "external_services_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.35,
            "retail_sales_vehicle_insurance_propensity_state": 0.65,
        },
        "platform_support_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.40,
            "retail_sales_vehicle_insurance_propensity_state": 0.60,
        },
        "project_management_cost_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.50,
            "retail_sales_vehicle_insurance_propensity_state": 0.50,
        },
    }
    income_rules = {
        "current_base_income_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.00,
            "retail_sales_vehicle_insurance_propensity_state": 0.00,
        },
        "new_clients_income_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 1.00,
            "retail_sales_vehicle_insurance_propensity_state": 0.00,
        },
        "insurance_income_czk": {
            "strategy_marketing_performance_acquisition_aggregation": 0.00,
            "retail_sales_vehicle_insurance_propensity_state": 1.00,
        },
    }

    def allocate(source: pd.DataFrame, scenario: str) -> pd.DataFrame:
        rows = []
        for _, row in source.iterrows():
            for product in products:
                allocated_cost = 0.0
                entry = {
                    "team_id": row["team_id"],
                    "scenario": scenario,
                    "period": row["period"],
                    "data_product": product,
                }
                for cost_col in cost_columns:
                    ratio = allocation_rules[cost_col][product]
                    value = round(float(row[cost_col]) * ratio, 2)
                    entry[cost_col] = value
                    allocated_cost += value
                attributed_income = 0.0
                for income_col, rules in income_rules.items():
                    value = round(float(row[income_col]) * rules[product], 2)
                    entry[income_col] = value
                    attributed_income += value
                entry["attributed_income_czk"] = round(attributed_income, 2)
                entry["allocated_cost_czk"] = round(allocated_cost, 2)
                entry["dq_flag"] = row["dq_flag"]
                rows.append(entry)
        return pd.DataFrame(rows)

    current_alloc = allocate(current_income_cost, "current_2025")
    plan_alloc = allocate(income_cost, f"plan_{PLAN_YEAR}")
    assumptions = pd.DataFrame(
        [
            {
                "cost_component": cost_component,
                "strategy_marketing_performance_acquisition_aggregation_share": rules[
                    "strategy_marketing_performance_acquisition_aggregation"
                ],
                "retail_sales_vehicle_insurance_propensity_state_share": rules[
                    "retail_sales_vehicle_insurance_propensity_state"
                ],
            }
            for cost_component, rules in allocation_rules.items()
        ]
    )
    return current_alloc, plan_alloc, assumptions


def write_outputs(
    ads_plan: pd.DataFrame,
    sessions: pd.DataFrame,
    conversions: pd.DataFrame,
    identity: pd.DataFrame,
    plan_clients: pd.DataFrame,
    plan_accounts: pd.DataFrame,
    current_clients: pd.DataFrame,
    current_accounts: pd.DataFrame,
    transactions: pd.DataFrame,
    vehicle_frame: pd.DataFrame,
    targeting: pd.DataFrame,
    segment_summary: pd.DataFrame,
    metrics: pd.DataFrame,
    acquisition_plan: pd.DataFrame,
    insurance_plan: pd.DataFrame,
    lending_plan: pd.DataFrame,
    current_income_cost: pd.DataFrame,
    income_cost: pd.DataFrame,
    current_product_cost_allocation: pd.DataFrame,
    plan_product_cost_allocation: pd.DataFrame,
    product_cost_allocation_assumptions: pd.DataFrame,
) -> None:
    ads_plan[["date", "platform", "campaign_id", "campaign_name", "adset_id", "cost_czk", "clicks", "impressions", "currency", "dq_flag"]].to_csv(DATA_DIR / "bronze_ads_campaign_performance.csv", index=False)
    sessions.to_csv(DATA_DIR / "bronze_web_sessions.csv", index=False)
    conversions.to_csv(DATA_DIR / "bronze_web_conversions.csv", index=False)
    identity[["user_id", "hashed_email", "client_id", "dq_flag"]].to_csv(DATA_DIR / "bronze_identity_map.csv", index=False)
    plan_clients[
        [
            "client_id",
            "created_at",
            "status",
            "acquisition_channel",
            "onboarding_mode",
            "mobile_app_user",
            "hashed_email",
            "dq_flag",
        ]
    ].to_csv(DATA_DIR / "bronze_plan_digital_clients.csv", index=False)
    plan_accounts[["account_id", "client_id", "created_at", "activated_at", "status", "product_type", "dq_flag"]].to_csv(
        DATA_DIR / "bronze_plan_digital_accounts.csv", index=False
    )
    current_clients[
        [
            "client_id",
            "created_at",
            "status",
            "acquisition_channel",
            "onboarding_mode",
            "mobile_app_user",
            "hashed_email",
            "dq_flag",
        ]
    ].to_csv(DATA_DIR / "bronze_crm_clients.csv", index=False)
    current_accounts[["account_id", "client_id", "created_at", "activated_at", "status", "product_type", "dq_flag"]].to_csv(DATA_DIR / "bronze_crm_accounts.csv", index=False)
    transactions.to_csv(DATA_DIR / "bronze_transactions.csv", index=False)
    vehicle_frame.to_csv(DEMO_DIR / "vehicle_training_frame.csv", index=False)
    targeting.to_csv(DEMO_DIR / "vehicle_targeting_top200.csv", index=False)
    segment_summary.to_csv(DEMO_DIR / "vehicle_segment_summary.csv", index=False)
    metrics.to_csv(DEMO_DIR / "vehicle_demo_metrics.csv", index=False)
    acquisition_plan.to_csv(DEMO_DIR / "plan_acquisition_monthly.csv", index=False)
    insurance_plan.to_csv(DEMO_DIR / "plan_insurance_targets.csv", index=False)
    lending_plan.to_csv(DEMO_DIR / "plan_lending_targets.csv", index=False)
    current_income_cost.to_csv(DATA_DIR / f"{TEAM_ID}_income_cost_current_2025.csv", index=False)
    income_cost.to_csv(DATA_DIR / f"{TEAM_ID}_income_cost_{PLAN_YEAR}.csv", index=False)
    current_product_cost_allocation.to_csv(DATA_DIR / f"{TEAM_ID}_product_cost_allocation_current_2025.csv", index=False)
    plan_product_cost_allocation.to_csv(DATA_DIR / f"{TEAM_ID}_product_cost_allocation_{PLAN_YEAR}.csv", index=False)
    product_cost_allocation_assumptions.to_csv(DEMO_DIR / f"{TEAM_ID}_product_cost_allocation_assumptions.csv", index=False)
    suite = {
        "suite_name": f"{TEAM_ID}_income_cost_dq_suite",
        "expectations": [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "team_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "period"}},
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "dq_flag"}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "income_czk", "min_value": 0, "allow_cross_type_comparisons": True}},
            {"expectation_type": "expect_column_values_to_be_between", "kwargs": {"column": "cost_czk", "min_value": 0, "allow_cross_type_comparisons": True}},
        ],
    }
    (DEMO_DIR / f"{TEAM_ID}_income_cost_dq_suite.json").write_text(json.dumps(suite, indent=2), encoding="utf-8")


def main():
    ensure_dirs()
    ads_plan = build_ads_plan()
    sessions, conversions, identity, plan_clients, plan_accounts = build_plan_web_events(ads_plan)
    current_clients, current_accounts = build_current_base_clients()
    transactions, labels = build_current_transactions(current_clients)
    vehicle_frame = build_vehicle_frame(current_clients, transactions, labels)
    _, targeting, segment_summary, metrics = run_propensity_demo(vehicle_frame)
    acquisition_plan = build_acquisition_plan_monthly(ads_plan, sessions, conversions, identity)
    insurance_plan = build_insurance_plan()
    lending_plan = build_lending_plan()
    current_income_cost = build_current_income_cost_2025()
    income_cost = build_income_cost(ads_plan, acquisition_plan)
    current_product_cost_allocation, plan_product_cost_allocation, product_cost_allocation_assumptions = build_product_cost_allocation(
        current_income_cost, income_cost
    )
    write_outputs(
        ads_plan,
        sessions,
        conversions,
        identity,
        plan_clients,
        plan_accounts,
        current_clients,
        current_accounts,
        transactions,
        vehicle_frame,
        targeting,
        segment_summary,
        metrics,
        acquisition_plan,
        insurance_plan,
        lending_plan,
        current_income_cost,
        income_cost,
        current_product_cost_allocation,
        plan_product_cost_allocation,
        product_cost_allocation_assumptions,
    )
    metadata = {
        "snapshot_date": str(SNAPSHOT_DATE.date()),
        "plan_year": PLAN_YEAR,
        "current_clients_base": CURRENT_CLIENTS,
        "current_insured_clients": CURRENT_INSURED_CLIENTS,
        "current_insured_share": round(CURRENT_INSURED_CLIENTS / CURRENT_CLIENTS, 4),
        "current_digital_onboarding_share": round(float(current_clients["onboarding_mode"].eq("pure_digital").mean()), 4),
        "current_assisted_onboarding_share": round(float(current_clients["onboarding_mode"].eq("assisted").mean()), 4),
        "current_mobile_app_user_share": round(float(current_clients["mobile_app_user"].mean()), 4),
        "target_new_clients": TARGET_NEW_CLIENTS,
        "target_digital_onboardings": TARGET_DIGITAL_ONBOARDINGS,
        "target_assisted_onboardings": TARGET_ASSISTED_ONBOARDINGS,
        "target_total_client_base": CURRENT_CLIENTS + TARGET_NEW_CLIENTS,
        "target_total_insured_clients": TARGET_INSURED_CLIENTS_TOTAL,
        "insurance_gap_to_close": TARGET_NEW_INSURED_CLIENTS,
        "current_loan_volume_czk": CURRENT_LOAN_VOLUME_CZK,
        "target_loan_volume_czk": TARGET_LOAN_VOLUME_CZK,
        "required_avg_ticket_without_mortgage": REQUIRED_AVG_TICKET_WITHOUT_MORTGAGE,
        "marketing_spend_czk": round(float(ads_plan["cost_czk"].sum()), 2),
        "planned_digital_cpa_czk": round(float(ads_plan["cost_czk"].sum()) / TARGET_DIGITAL_ONBOARDINGS, 2),
        "vehicle_positive_rate_current_base": round(float(vehicle_frame["target_buy_vehicle_insurance_30d"].mean()), 4),
        "current_base_vehicle_insurance_share": round(float(labels["has_vehicle_insurance"].mean()), 4),
    }
    (DEMO_DIR / "generation_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
