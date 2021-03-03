CREATE KEYSPACE IF NOT EXISTS feature_store
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };