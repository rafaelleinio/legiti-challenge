CREATE TABLE IF NOT EXISTS feature_store.user_chargebacks (
    cpf text PRIMARY KEY,
    timestamp timestamp,
    cpf_chargebacks__count_over_3_days_rolling_windows int,
    cpf_chargebacks__count_over_7_days_rolling_windows int,
    cpf_chargebacks__count_over_30_days_rolling_windows int
);
