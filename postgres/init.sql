-- One Postgres instance locally, one database per service (separate schemas, no shared tables).
CREATE DATABASE orders;
CREATE DATABASE payments;
CREATE DATABASE ledger;
CREATE DATABASE reconciliation;
CREATE DATABASE keycloak;
