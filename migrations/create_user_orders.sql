CREATE TABLE IF NOT EXISTS feature_store.user_orders (
    cpf text PRIMARY KEY,
    timestamp timestamp,
    cpf_orders__count_over_3_days_rolling_windows int,
    cpf_orders__count_over_7_days_rolling_windows int,
    cpf_orders__count_over_30_days_rolling_windows int
);
