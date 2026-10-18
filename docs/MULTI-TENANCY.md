# Multi-tenancy: what exists, the options, and what I would choose

A design note, not an implementation. Nothing here has been built beyond what the first section says is already there.

## What the platform does today

A "tenant" in the sense of "a business that uses the platform" is a **merchant**. Isolation today is by convention, in
three places:

| Layer | How merchants are separated | Where |
|---|---|---|
| Money | one Fineract savings account per merchant; `merchant_accounts` in ledger-service maps `merchant_id` to the account | `services/ledger-service` |
| Load | a Redis token bucket per merchant, with a per-merchant override, so one merchant flooding the API does not slow the others (tested) | `libs/platform-common/RateLimiter`, [DESIGN.md](DESIGN.md) section 7 |
| Data | `merchant_id` is a column on orders and ledger entries; there is no database-enforced separation | `services/order-service`, `services/ledger-service` |

Known gap: the merchant comes from the `X-Merchant-Id` header, which a caller can spoof. Until it comes from a verified
token, none of the isolation above is a security boundary, only a fairness and bookkeeping one.

Fineract has its own tenant mechanism and the platform uses only the default one. A request carries
`Fineract-Platform-TenantId`; Fineract looks the identifier up in the `tenants` table of the `fineract_tenants` database
and connects to the database named in `tenant_server_connections` (checked on the running stack: one tenant, `default`,
database `fineract_default`). So Fineract's model is **one database per tenant**, each with its own copy of the schema.

## The options

| | Row-level (shared tables, `tenant_id` column) | Schema or database per tenant |
|---|---|---|
| Isolation | By query discipline, or by Postgres row-level security (RLS) if every connection sets the tenant | By the database: a missing `WHERE` cannot leak another tenant's rows |
| Cost per tenant | A few rows | A schema and its migrations; a database also costs connections and memory |
| Noisy neighbour | Shared tables and indexes; one large tenant slows everyone | Separate tables; can be moved to its own server |
| Migrations | Once | Once per tenant; a failed one leaves tenants on different versions |
| Cross-tenant reporting | Trivial | A union over tenants, or the analytics layer |
| Delete or export one tenant | `DELETE ... WHERE tenant_id` everywhere, easy to miss a table | Drop the schema or the database |
| Fits Fineract | No: Fineract does not do it | Yes: it is Fineract's own model |

## The ledger: merchants as accounts, or a Fineract tenant per merchant

- **One Fineract tenant, one savings account per merchant (today).** Cheap. One schema to migrate. Merchants share
  Fineract's tables, so a bug in how the account is chosen is a wrong-merchant booking, not an error. Transfers between
  merchants stay possible.
- **One Fineract tenant per merchant.** The database enforces the separation and a merchant can be exported or deleted as
  a unit. It costs a database and its migrations per merchant, and each tenant needs its own connection pool in Fineract.
  Transfers between merchants become cross-tenant, which Fineract does not do.

## What I would choose

For a payments platform of this size: **keep one Fineract tenant, and make the merchant a verified identity.** Order of
work:

1. Take the merchant from a verified JWT claim, never from a header. Without this the other items protect nothing.
2. Add `merchant_id` to every query path that returns data, and add Postgres row-level security on the platform tables
   (orders, payments, ledger entries) as a second wall, with the service connecting as a role that must set
   `app.merchant_id` per transaction. A missing filter then returns nothing instead of another merchant's rows.
3. Move to a Fineract tenant per merchant only for a merchant with a contractual need for separation (data residency,
   its own export or deletion, a regulator). That is a per-merchant decision, not a platform-wide one.

Schema-per-tenant for the platform's own databases is the other reasonable choice. It would be right if tenants were
few and large; here merchants are many and small, which favours rows plus RLS.

## What it means for the data platform

- Change data capture is per database: with one tenant there are four connectors; with a Fineract database per merchant
  it would be one more connector (and one more replication slot) per merchant, which is the real cost of that option.
- The lake and the dbt models would add `merchant_id` as a partition and a column, and the daily metrics would be per
  merchant. `orders` and `ledger_entries` already carry it; the Fineract tables reach it through the savings account.
- The reconciliation joins on `payment_id`, which is globally unique, so it does not depend on the tenancy model.
