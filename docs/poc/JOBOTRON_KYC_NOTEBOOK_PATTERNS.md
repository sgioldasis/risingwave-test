# JOBOTRON_KYC_NOTEBOOK_PATTERNS.md — the KYC team's Jobotron notebooks, and the design of the component that replaces them

**Status: design brief (2026-11-11).** Source: the KYC team's notebooks in
`gitlab.com/kaizengaming/projects/accounts/kyc/kyc-reports` → `databricks/notebooks/jobotron/`
(read via SSH shallow clone; the web UI sits behind SAML SSO and the local `GITLAB_TOKEN` is
expired). Context: the vendored publish component is
[`kaizen-dagster-delta-to-kafka` 0.1.0](REVERSE_ETL_JOBOTRON.md) (MR !166); the team tracks
"Productionise Delta-to-Kafka" next sprint. **Decision (2026-11-11): the component that replaces
the notebooks is a new wheel — `kaizen-dagster-jobotron` — not an extension of the team's
`delta-to-kafka`** (theirs stays untouched and generic). This doc is its design brief: §1–§3 are
the evidence (notebook analysis), §4 the design, §5–§6 the boundaries and guards.

## 1. The anatomy of every notebook

```
companies (enumerate active) → [resolve parameters] → per-company SQL (comp_part-pruned)
  → publish (13-field envelope, key=CustomerId) → the Worker acts and marks done
```

The variants differ only in **how parameters are resolved**:

| Variant | Notebooks | Parameter resolution |
|---|---|---|
| **A. Static single-company** | `send-email-to-unverified-customers/unverified_customers_{cy,gr,ro}` | Hardcoded `companyId` + literals in the SQL. **Exactly what our three demo `defs.yaml` instances already do.** |
| **B. Configurator-driven fan-out** | `limit-unverified-customer-rights`, `notify-unverified-customers-over-deposit-limit` | Loop **all active companies**; each company's parameters come from its Configurator entry (JSON: `isActive`, `group`, `daysCountdown`), group code → id via `mgroup` |
| **C. Group-based fan-out** | `send-notification-to-verified-customers-with-expired-id` | Loop all active companies; one constant group id; per-company query only |
| *(D. One-off)* | `send-sms-to-kyc-blocked-customers` | Single company, `limit 1000` per run, "not already in group" negative check |

## 2. The shared primitives (the automatable pattern)

1. **Company enumeration** — `de.metadatadb.mcompany` minus a static inactive list
   (`2, 3, 5, 6, 10, 12, 13, 14`). Identical in every fan-out notebook (`companies.ipynb`).
2. **`comp_part` partition pruning** — every query filters `comp_part = {companyId}`. This is
   *the* performance trick (read one company's shard instead of all ~29) **and** the isolation
   guarantee (another company's customers cannot leak into a company's topic).
3. **Configurator client** (`configurator_client.ipynb`) — plain
   `GET {base}/api/Sdk/calculateValue?configurationCode=…&hierarchyNodeCode=…&applicationName=databricks`,
   **no authentication** (access = network position). Connect-retry: 3 attempts, 5 s/15 s delays,
   connect-only (a request that reached the server is never retried). The JSON value is read very
   defensively: field names matched case-insensitively, every field parsed as text then converted
   (the configurator may return `"isActive": "true"`), and **an inactive config is a valid skip,
   not an error**.
4. **Group resolution** (`groups.ipynb`) — `mgrCode` → `mgrGroupID` from `de.metadatadb.mgroup`;
   fail on 0 or >1 matches; resolved through the DataFrame API (parameter binding), never SQL
   string interpolation, because the code comes from configuration.
5. **Failure isolation loop** — per company: catch `NonFatal`, keep going, aggregate failed
   company ids, `require(failedCompanyIds.isEmpty)` at the end. One company's bad config must not
   block the other ~28.
6. **Publish semantics** (`kafka_message_publisher.ipynb`) — the full eligible set every run,
   one message per customer (`distinct`), dedup delegated to the Worker; the Worker marks done via
   `cCustomer_Group` (`ccgValue0 = 1` / group membership), which is *why* republishing is safe.
   `localCheckpoint()` materializes the snapshot once so the count and the rows come from the same
   query run. Topics are **per company**: `kyc.jobs.<first two chars of comCode, lowercased>`.
7. **Dry-run** — a `IsDryRun` widget, default `true` (same concept as the component's
   `dry_run`).
8. **PII minimization** — only `CustomerId`/`CompanyId` are selected: usernames, phone numbers
   and emails deliberately stay out of Kafka; the Worker/Iris look them up themselves.
9. **The 13-field envelope** — every job notebook publishes the full envelope (`JobId`,
   `JobName`, `JobCreated`, `RunId`, `ItemsCount`, `ItemNumber`, `ItemId`, `ItemPayload`,
   `ItemPayloadHash`, `IsDryRun`, `SchemaVersion`) onto the `kyc.jobs.*` topics; a repo-wide
   check found no other publishing path. Verified across all 7 job notebooks: they all `%run`
   the one shared publisher and call `publishRows`. The team **descoped** this envelope from
   their delta-to-kafka component (plain JSON instead) — the new wheel regains it (§4.5).

## 3. What the existing component already covers

`DeltaToKafkaComponent` (vendored 0.1.0) **is** variant A: query → Statement Execution →
one JSON message per row → Kafka, keyed by `key_column`, with per-run `dry_run`. Our three demo
instances use it exactly that way. It is the starting point the new wheel supersedes.

## 4. The component design (`kaizen-dagster-jobotron` / `JobotronComponent`)

**One component type, no variants in code.** The type is
`kaizen_dagster_jobotron.components.jobotron.JobotronComponent`; the distribution is a **new
wheel** (coexistence with the team's `delta-to-kafka` is trivial — different distributions — and
the name is honest: everything beyond plain publish is Jobotron domain logic). The `companies`
block is the mode switch: absent = single statement (variant A's shape; the demo's three
instances migrate by swapping the type line), present = fan-out — and **fan-out means a
partitioned asset: one Dagster partition per company**. The partition is the loop iteration:
each company materializes independently (its own statement, topic, status), so selective
rematerialization ("re-run just GR"), backfills, and per-company observability are Dagster
native — the notebooks' per-company failure isolation, expressed as partitions.

### 4.1 Design principles (derived from the evidence)

1. **The notebooks are the spec.** Every behavior in the component has a corresponding notebook
   behavior; nothing else goes in.
2. **Pure core, thin edges.** The contract logic (envelope building, template binding, parameter
   resolution) is pure and testable; only execution and sink touch I/O.
3. **One component, one mode switch** (above).
4. **Safe by default**: `dry_run: true` in YAML, dry-run builds everything and sends nothing, a
   publish to a nonexistent topic fails fast.
5. **Fail loud at load, isolate at run.** Config errors surface at `dg check`/definition-load
   time; runtime failures isolate per company and aggregate.

### 4.2 Architecture: four layers

```
┌─────────────────────────────────────────────────────────────┐
│ RESOLUTION — "which companies, with which parameters"       │
│   company source → partition set + per-company resolvers    │
├─────────────────────────────────────────────────────────────┤
│ EXECUTION — one bound statement per company                 │
│   template + bindings → Statement Execution → result set    │
├─────────────────────────────────────────────────────────────┤
│ CONTRACT — pure message building (the envelope)             │
│   result set + run context → messages                       │
├─────────────────────────────────────────────────────────────┤
│ SINK — Kafka profile, topic routing, produce                │
└─────────────────────────────────────────────────────────────┘
```

Each layer maps to one notebook primitive (§2) and is independently testable. The resolution and
contract layers are pure; execution and sink are the only I/O seams.

### 4.3 The parameter algebra (the heart of the design)

The variants differ *only* in how parameters resolve. Model it as a small set of composable
resolvers, each producing values for the template's `{placeholders}`:

| Resolver | Produces | Notebook origin |
|---|---|---|
| **static** (`parameters:`) | constants for all companies | variant A/C/D literals |
| **per-company static** (`parameters_by_company:`) | per-company constants | the cy/gr/ro trio's 4-line differences |
| **configurator** (`configurator:`) | per-company values from its config entry; **inactive = successful skip** | `configurator_client.ipynb` |
| **group lookup** (`group_lookup:`) | resolves a code-valued variable → id via `mgroup`; fails on 0 or >1 matches | `groups.ipynb` |
| **run anchor** (built-in) | run-scoped values (`{created_from}`, `{run_ts}`) evaluated **once per run** | the `createdFrom` invariant |

Resolution order is fixed: run anchor → static → per-company static → configurator → group
lookup (each may consume the previous layer's variables). Every template variable must have
exactly one source: two resolvers producing the same variable, or a variable nobody produces, is
a **load-time error**.

This algebra is the whole variant story: A = static; C = static + companies; B = configurator +
group; D = static single company with a `limit` in the template; the demo trio = per-company
static. Each notebook becomes ~15 lines of YAML instead of ~100 lines of Scala.

### 4.4 The YAML surface (complete)

```yaml
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: unverified_customers
  job_name: Send_Email_to_Unverified_Customers   # the envelope's JobName/JobId prefix — the
                                                  # NOTEBOOK's name, not a YAML slug
  asset_key_prefix: [jobotron]

  # EXECUTION input
  query_template: |                        # must pass the comp_part guard in fan-out mode
    select distinct c.cusCustomerID as CustomerId, c.cusCompanyID as CompanyId
    from cCustomer_Group g
    inner join cCustomer c on c.cusCustomerID = g.ccgCustomerID
    where g.comp_part = {company_id} and c.comp_part = {company_id}
    and g.ccgGroupID = {group_id}
    and g.ccgCreated >= timestamp'{created_from}'

  # RESOLUTION
  companies:                               # ABSENT = single-statement mode;
    source: de.metadatadb.mcompany         # present = fan-out AND the partition set
    exclude: [2, 3, 5, 6, 10, 12, 13, 14]  # (one partition per company; from a static
                                           #  include list -> StaticPartitionsDef, from the
                                           #  mcompany source -> DynamicPartitionsDef)
  configurator:                            # optional; requires companies
    base_url: "{{ env('CONFIGURATOR_BASE_URL') }}"
    configuration_code: UnverifiedCustomerLimitsUntilRestrictionConfig
    fields: {group_code: group, days_countdown: daysCountdown}   # isActive is the implicit gate
  group_lookup:
    code: group_code                       # resolves {group_code} → {group_id}
  parameters: {group_id: 1416}             # optional static binds, all companies
  # parameters_by_company:                 # optional per-company static binds (fan-out mode)
  #   7: {group_id: 1424, excluded_types: "2, 5, 6, 10, 11", market: cy}
  #   1: {group_id: 1425, excluded_types: "2, 5, 6, 10, 11, 15", market: gr}

  # CONTRACT
  message:
    format: envelope                       # or json (plain, demo mode)
    key_column: CustomerId
  schema_version: 1                        # envelope only; bumped by the author on a contract extension

  # SINK
  kafka_profile: openshift_kafka_connect
  topic_template: "kyc.jobs.{market_code}"    # single-statement mode: plain `topic:`
  metadata: [processed_at, run_id]         # plain-JSON mode only

  workspace: "{{ env('DBT_DATABRICKS_HOST') }}"
  warehouse: "{{ env('DATABRICKS_WAREHOUSE_ID') }}"
  dry_run: true
```

### 4.5 The envelope contract

**The component emits the 13-field envelope (§2.9) — plain JSON is not acceptable for replacing
the notebooks**, whose Workers parse it today (dedup on `ItemId`, progress on
`ItemsCount`/`ItemNumber`, no-op on `IsDryRun`). Plain-JSON mode exists for the demo; envelope is
the contract mode. Everything it entails is already implemented in `kafka_message_publisher.ipynb`
— the wheel ports, not invents:

- **Canonical JSON + hash**: `ItemPayload` serialized with `ignoreNullFields=false`, sha2-256 →
  `ItemPayloadHash`; `ItemId = "<hash>-<ItemNumber>"`.
- **A materialized per-run snapshot**: `ItemsCount` and every `ItemNumber` come from one query
  run (the notebook's `localCheckpoint()`); the component's per-company statement result *is*
  that snapshot.
- **Run identity**: `RunId` (one per run), `JobCreated` (run start, second-truncated UTC),
  `JobId = "{jobName}_{companyId}_{jobDateTime}"`, `IsDryRun` echoed from the run config.
- **Deterministic ordering**: `ItemNumber` via row_number over `CustomerId` — stable across
  reruns of the same input.
- `key` stays the stringified `CustomerId` (the envelope fixes identity in two places — key and
  field — deliberately).

**Dry-run semantics (decision 2026-11-12): notebook semantics.** In the notebooks, `IsDryRun`
is *just an envelope field*: `publish()` sends the messages to Kafka regardless, and the
**Worker** no-ops `IsDryRun: true` batches. The component keeps that: `dry_run: true` builds and
**publishes** with `IsDryRun: true` — it does not send nothing. Confirmed empirically: a live
message read off `kyc.jobs.br` (2026-11-11) carries `"IsDryRun": true` from that day's
`Limit_Unverified_Customer_Rights` run — the notebook's dry run really writes to the topic.
Consequences: (1) a dry run touches the real topic, so the topic preflight and the
topics-own-resolution guard apply to dry runs too; (2) the demo's consume mirror, standing in
for the Worker, must **skip `IsDryRun: true` messages** once on the envelope — otherwise demo
dry runs would pollute the mirror counts.

Compatibility guarantee: **byte-for-byte golden tests** against the notebook publisher's output
(§4.9), anchored on a **live specimen captured 2026-11-12** from `kyc.jobs.br` (offset 52): all
13 fields, exact names and casing, `JobId = "{job_name}_{companyId}_{yyyyMMddHHmmss}"`,
`JobCreated` second-truncated ISO with `Z`, `ItemId = "{hash}-{ItemNumber}"`, `ItemPayload`
inline ints, key = stringified `CustomerId`. The fixture lives with the wheel's tests
(sanitized — fake ids, real shape). Consequence for **our demo**: the mirror currently dedups on
the Kafka key under the vendored wheel's plain JSON; on migration the instances switch to
`format: envelope` and the mirror's dedup moves back to `ItemId` — the pre-adoption behavior,
restored by configuration.

### 4.6 Variant configurations (the acceptance criteria)

**Variant A — static single-company** (originally the demo's shape; the demo has since
consolidated its three variant-A instances into the Alternative's ONE fan-out —
[`unverified_customers/defs.yaml`](../../../orchestration/defs/unverified_customers/defs.yaml)
— so this single-company form is the shape a new sibling notebook would start from):

```yaml
# no `companies` block = single-statement mode
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: unverified_customers_cy
  asset_key_prefix: [jobotron]
  query: |
    select cusCustomerID as CustomerId, cusCompanyID as CompanyId
    from de.voperdb_cy.cCustomer
    inner join de.metadatadb.mVerificationStatus on cusVerificationStatusID = vstVerificationStatusID
    left join de.voperdb_cy.cCustomer_Group on ccgCustomerID = cusCustomerID and ccgGroupID = 1424
    where cusCreated between current_timestamp() - interval 28 days and current_timestamp() - interval 26 days
    and cusTypeID not in (2, 5, 6, 10, 11)  -- Test, RISK BLOCK, UAT, UNVERIFIED BETANO-RO
    and cusCompanyID = 7  -- CY
    and cusStatusID = 3  -- REAL
    and vstVerifiedFL = false  -- Not verified
    and (ccgValue0 is null or ccgValue0 <> '1')
  workspace: "{{ env('DBT_DATABRICKS_HOST') }}"
  warehouse: "{{ env('DATABRICKS_WAREHOUSE_ID') }}"
  kafka_profile: openshift_kafka_connect
  topic: jobotron_unverified_customers_cy
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  dry_run: true
```

**Alternative — the same three notebooks (cy, gr, ro) as ONE fan-out instance.** The template
becomes the shared 90%; the per-company differences become data:

```yaml
# the THREE variant-A notebooks (unverified_customers_{cy,gr,ro}) as ONE instance.
# {market} is a demo-ism (the shard-name routing key de.voperdb_<market>): production
# queries run the shared tables with comp_part = {company_id} bound instead (§4.6).
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: unverified_customers
  asset_key_prefix: [jobotron]
  query_template: |
    select cusCustomerID as CustomerId, cusCompanyID as CompanyId
    from de.voperdb_{market}.cCustomer
    inner join de.metadatadb.mVerificationStatus on cusVerificationStatusID = vstVerificationStatusID
    left join de.voperdb_{market}.cCustomer_Group on ccgCustomerID = cusCustomerID and ccgGroupID = {group_id}
    where cusCreated between current_timestamp() - interval 28 days and current_timestamp() - interval 26 days
    and cusTypeID not in ({excluded_types})
    and cusCompanyID = {company_id}
    and cusStatusID = 3  -- REAL
    and vstVerifiedFL = false  -- Not verified
    and (ccgValue0 is null or ccgValue0 <> '1')
  topic_template: "jobotron_unverified_customers_{market}"
  companies:
    source: de.metadatadb.mcompany
    include: [7, 1, 4]                # CY, GR (GML), RO — in mcompany order
  parameters_by_company:
    7: {market: cy, group_id: 1424, excluded_types: "2, 5, 6, 10, 11"}
    1: {market: gr, group_id: 1425, excluded_types: "2, 5, 6, 10, 11, 15"}   # +15: UNVERIFIED STOIXIMAN-GR
    4: {market: ro, group_id: 1321, excluded_types: "2, 5, 6, 10, 11"}
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  workspace: "{{ env('DBT_DATABRICKS_HOST') }}"
  warehouse: "{{ env('DATABRICKS_WAREHOUSE_ID') }}"
  kafka_profile: openshift_kafka_connect
  dry_run: true
```

Three statements, three topics (`jobotron_unverified_customers_{cy,gr,ro}`) — one partitioned
asset with three partitions, replacing three notebooks and three single-company instances. The
per-company-static form is the production default for sibling notebooks; the three separate
variant-A instances remain the demo's shape (its per-market plumbing predates the component).

**Variant B — configurator-driven fan-out** (replaces BOTH
`limit-unverified-customer-rights.ipynb` and
`notify-unverified-customers-over-deposit-limit.ipynb`; shown for the latter — the former
differs in the template and one config field):

```yaml
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: notify_unverified_customers_over_deposit_limit
  query_template: |
    select distinct c.cusCustomerID as CustomerId, c.cusCompanyID as CompanyId
    from cCustomer_Group g
    inner join cCustomer c on c.cusCustomerID = g.ccgCustomerID
    where g.comp_part = {company_id}
    and c.comp_part = {company_id}
    and g.ccgGroupID = {group_id}   -- from the configurator's 'group' code, resolved via mgroup
    and g.ccgCreated >= timestamp'{created_from}'   -- run-scoped anchor: start of yesterday, UTC
  topic_template: "kyc.jobs.{market_code}"
  companies:
    source: de.metadatadb.mcompany
    exclude: [2, 3, 5, 6, 10, 12, 13, 14]
  configurator:
    base_url: "{{ env('CONFIGURATOR_BASE_URL') }}"
    configuration_code: UnverifiedCustomerLimitsUntilRestrictionConfig
    fields: {group_code: group}     # config key 'group' -> template variable {group_code}; isActive is the implicit gate
  group_lookup:
    code: group_code                # resolve {group_code} (a mgrCode string) -> binds {group_id} from mgroup
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  dry_run: true
```

**Variant B′ — the sibling variant-B job** (`limit-unverified-customer-rights.ipynb`): the same
shape as variant B, differing in three lines — an extra config field, two template predicates,
and the job name:

```yaml
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: limit_unverified_customer_rights
  query_template: |
    select distinct c.cusCustomerID as CustomerId, c.cusCompanyID as CompanyId
    from cCustomer_Group g
    inner join cCustomer c on c.cusCustomerID = g.ccgCustomerID
    where g.comp_part = {company_id}
    and c.comp_part = {company_id}
    and g.ccgGroupID = {group_id}   -- from the configurator's 'group' code, resolved via mgroup
    and g.ccgCreated < current_timestamp() - interval {days_countdown} days   -- the countdown elapsed
    and g.ccgValue4 is null         -- not yet restricted (the Worker marks here)
  topic_template: "kyc.jobs.{market_code}"
  companies:
    source: de.metadatadb.mcompany
    exclude: [2, 3, 5, 6, 10, 12, 13, 14]
  configurator:
    base_url: "{{ env('CONFIGURATOR_BASE_URL') }}"
    configuration_code: UnverifiedCustomerLimitsUntilRestrictionConfig
    fields: {group_code: group, days_countdown: daysCountdown}
  group_lookup:
    code: group_code
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  dry_run: true
```

The two ~100-line Scala notebooks become two YAMLs differing in three lines: the job name, the
`days_countdown` field, and the two template predicates that use it.

**Variant C — group-based fan-out** (replaces
`send-notification-to-verified-customers-with-expired-id.ipynb`; no configurator, the group id
is a constant):

```yaml
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: verified_customers_with_expired_id
  query_template: |
    select distinct ccgCustomerID as CustomerId, cusCompanyID as CompanyId
    from cCustomer_Group
    inner join cCustomer on cusCustomerID = ccgCustomerID
    where ccgGroupID = {group_id}
    and ccgValue0 is null            -- Not notified yet
    and ccgCreated < current_timestamp() - interval 30 days
    and cusCompanyID = {company_id}
  topic_template: "kyc.jobs.{market_code}"
  companies:
    source: de.metadatadb.mcompany
    exclude: [2, 3, 5, 6, 10, 12, 13, 14]
  parameters: {group_id: 1416}      # CustomersVerifiedIdentityExpired, same group in every company
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  dry_run: true
```

**Variant D — rate-limited single company** (replaces
`send-sms-to-kyc-blocked-customers/kyc_blocked_customers_gr.ipynb`; the fan-out machinery with a
one-company list — the `limit` and the eligibility `exists`/`not exists` clauses are just SQL in
the template, and the "rest go out on the next runs" semantics live in the Worker's marking):

```yaml
type: kaizen_dagster_jobotron.components.jobotron.JobotronComponent
attributes:
  name: kyc_blocked_customers_gr
  query_template: |
    select a.cusCustomerID as CustomerId, a.cusCompanyID as CompanyId
    from cCustomer a
    inner join mVerificationStatus on cusVerificationStatusID = vstVerificationStatusID
    where cusCompanyID = {company_id}
    and cusCountryID = 1
    and cusStatusID = 3             -- REAL
    and current_timestamp() > cusCreated + interval 1 month - interval 2 days
    and vstVerifiedFL = false
    and coalesce(trim(cusMobilePhone), '') <> ''   -- the number itself stays out of Kafka
    and exists (
      select 1 from tTransaction
      where traCustomerID = a.cusCustomerID and traStatusID = 3
      and traTypeID not in (22, 23, 70)
      and traCreated > current_timestamp() - interval 1 month
      and traHashValue = a.cusCustomerID % 64)
    and not exists (
      select 1 from cCustomer_Group
      where ccgCustomerID = a.cusCustomerID
      and ccgGroupID = {group_id})  -- Already sent the SMS
    limit 1000                      -- The procedure's TOP 1000; the rest go out on the next runs
  topic_template: "kyc.jobs.{market_code}"
  companies:
    source: de.metadatadb.mcompany
    include: [1]                    # Stoiximan GR only
  parameters: {group_id: 1127}      # KYC_SMS_Sent: the Worker adds every customer it SMSes
  message: {format: envelope, key_column: CustomerId}
  schema_version: 1
  dry_run: true
```

All four carry the guards automatically: parameter binding for every `{placeholder}` sourced
from config, the run-scoped `{created_from}`-style anchor resolved once per run, and the
`comp_part = {company_id}` lint (variant D's template satisfies it via `cusCompanyID =
{company_id}`; the lint accepts any `{company_id}` filter, `comp_part` being the canonical form).

**Collapsing siblings**: one instance per **job**, not per company — the production rule. The
unverified-customers trio collapses into one partitioned fan-out instance (the alternative form
under variant A above: shared template, `companies: {include: [7, 1, 4]}`,
`parameters_by_company:` carrying the four-line differences). The same collapse applies to the
two variant-B notebooks (they share one config code and nearly the same template). Statements,
topics and per-partition outcomes stay per company.

**Production posture vs demo scaffolding.** This design targets production; the demo is a test
harness. Three demo-only patterns must not leak into it: (1) per-market instances — production
has no "market" concept, only companies (`mcompany`, `{company_id}`, `{com_code}`, `comp_part`);
(2) the per-market shard-qualified tables (`de.voperdb_cy.…`) — production queries run the
shared tables with `comp_part = {company_id}` bound, and the demo's table-name sharding is a
warehouse workaround, not a design element; (3) the demo's per-market asset names and mirror
tables — scaffolding that predates the component. What production adds that the demo does not
exercise: environment-specific `kafka_profiles` (stg/prd bootstrap, SSL truststore vs SASL) and
`CONFIGURATOR_BASE_URL` per environment; **schedules** (these are daily jobs — the component
emits a partitioned asset, the host project wraps it in a job + schedule that materializes all
partitions, or a subset after an incident; no cron inside the component); and per-partition
status plus the outcome metadata (counts, statement seconds, skip reasons) as the ops surface
feeding monitoring. The §6 guards are hard requirements here: a production instance writes only
to the topics its own template resolution produces — never a hand-typed topic name.

### 4.7 Execution model

**Fan-out mode = a partitioned asset; the partition is the loop iteration.** One partition per
company (`companies` defines the partition set: a static `include`/`parameters_by_company` list
→ `StaticPartitionsDef`; the `mcompany` source → `DynamicPartitionsDef`, so companies joining or
leaving don't need a code deploy). Per materialized partition:

1. **Resolve the run anchor once** (`created_from`, `run_ts`, `run_id` — one value even across a
   midnight-crossing run).
2. **Resolve this company's parameters**: run anchor → static → per-company static →
   configurator → group lookup. Skip-with-note if the configurator says inactive (the partition
   materializes as a no-op success — a valid business case, noted in the partition metadata).
3. **Build the bound statement**: values bound via the Statement Execution API's parameters
   array, never interpolated — the one hard rule.
4. **Execute → build messages**: the result set *is* the snapshot — `ItemsCount` and
   `ItemNumber` (row_number over `CustomerId`) come from it, matching the notebook's
   `localCheckpoint()` semantics. `JobCreated`/`RunId`/`JobId` come from the run anchor;
   `IsDryRun` echoes the run config.
5. **Publish** to this company's routed topic; record per-partition outcome metadata — counts,
   statement seconds, topic, skip reasons.

What Dagster gives for free, replacing the notebooks' custom loop plumbing:

- **Failure isolation**: a company's failure fails *its partition*, not the run — the notebooks'
  `NonFatal` catch and `require(failedCompanyIds.isEmpty)` become native per-partition status.
- **Selective rematerialization**: re-run just GR after an incident, backfill a newly onboarded
  company, or materialize all partitions on the daily schedule (a host-level job + schedule
  choice).
- **Observability**: per-company status in the UI; the outcome metadata is the ops surface.

Within one run, partitions execute **sequentially by default** (determinism, warehouse load);
parallelism is a later executor concern, not a design pillar.

Single-statement mode is the degenerate case: one company, no partitions, one statement, one
topic — today's variant-A behavior exactly.

### 4.8 Error taxonomy (what fails when)

| Error class | When | Behavior |
|---|---|---|
| Schema/config errors | definition load / `dg check` | hard fail before anything runs: unknown template variables, duplicate variable sources, `configurator` without `companies`, fan-out template missing a `{company_id}` filter |
| Resolution errors | per partition | group code → 0 or >1 ids: **real failure** — the partition fails, others are unaffected |
| Inactive config | per partition | **no-op success**, skip-reason in the partition metadata (a valid business case) |
| Statement/publish errors | per partition | the partition fails with the reason; other partitions and the run continue |
| Run-level view | after a batch | a run materializing N partitions reports each one's status; there is no silent partial success — failed partitions are visible and individually re-runnable |

### 4.9 Testing strategy (the wheel's own tests, before any instance exists)

- **Contract tests** (pure, exhaustive): envelope field-for-field against a golden notebook
  output — build a message from a fixed input and compare byte-for-byte with what
  `kafka_message_publisher` produces (canonical JSON, `ignoreNullFields=false`, hash, `ItemId`
  composition). This is the compatibility guarantee to the Workers.
- **Resolver algebra tests**: each resolver pure with fakes (configurator fake returning entries
  per company; group fake resolving codes); combination and conflict tests.
- **Partition-semantics tests**: skip-vs-fail per the taxonomy; partition-set derivation from
  both static and source-based `companies` blocks; a mutation suite covering the dangerous
  defaults (an omitted `dry_run` is a dry run, publish without topic fails fast, skip reasons
  are never swallowed).
- **Live smoke**: the protocol from this repo's adoption — dry-run first, one partition, real
  publish, topic-count == partition-metadata-count round trip.

## 5. What the component does NOT do

- **The Worker side** — marking `ccgValue0`/group membership, Iris actions, emails/SMS. A
  Dagster component is the wrong shape for it: the component model rewards stateless, rerunnable,
  side-effect-free assets; the Worker *is* the stateful side-effecting party whose marking scheme
  makes republishing safe. The one legitimate Dagster adjacency is **monitoring** (topic consumer
  lag, per-job publish counts), not replacement. The notebooks deliberately keep PII out of Kafka
  so the Worker looks it up itself — that is Jobotron-the-system's job (RFC non-goals, unchanged).

## 6. Access check (2026-11-11, laptop on VPN)

| Building block | Result |
|---|---|
| Configurator stg (`configurator-host-webapi.apps.stgocp.stgkaizen.local`) | HTTP 200, **no auth** — the notebook's own example call (`JobotronSettings`/`kaizen` → `{"BaseAddress": null}`) |
| Configurator prd (`configurator.kaizengaming.net`) | HTTP 200, no auth, same response |
| STG Kafka (`stg-ocp-kfk01-bootstrap.kaizengaming.net:9096`, SASL/SCRAM) | Connected with the repo's `.env` credentials; 6 brokers, ~4,155 topics |
| The real KYC topics | `kyc.jobs.<cc>` exist for ~32 company codes, alongside our `jobotron_unverified_customers_{cy,gr,ro}` demo topics |
| Databricks workspace / warehouse | Already proven (the demo publishes through it) |

Notes and guards:

- **VPN is required** — the hostnames resolve through the VPN's scoped DNS only (they fail
  without it; note `dig` shows nothing even on VPN — macOS routes these domains through the
  system resolver, so test with `dscacheutil -q host -a name <host>`).
- The configurator being **unauthenticated** means the component should not log config values
  wholesale (network-position access = anyone on the network can read any company's config).
- **The `kyc.jobs.*` topics are live** — KYC Workers consume them and act on real customers
  (emails, SMS, limits). A misconfigured real publish there triggers real actions. Any
  experiment must stay on the `jobotron_*` demo topics — encode that as an allowlist guard
  (same idea as the demo's reset-name allowlist).

## 7. Pre-build questions — answered (2026-11-12)

1. **Which envelope fields does the Worker actually require?** → Moot for building: the
   publisher's own Scala is the spec, a **live specimen was captured from `kyc.jobs.br`**
   (§4.5), and the decision is to emit the full envelope byte-compatibly. The Worker's minimal
   field set matters only if a future slimming is proposed — the team's call, not a blocker.
2. **Dry-run semantics** → **notebook semantics** (§4.5): dry run publishes with `IsDryRun:
   true`; the Worker no-ops it. Confirmed empirically by the captured specimen.
3. **Statement Execution parameter binding** → **works**: a live spike executed
   `select :company_id …, :group_code …` with `StatementParameterListItem` parameters against
   the demo warehouse — `SUCCEEDED`, values bound correctly (INT and STRING). The "bind, never
   interpolate" rule (§4.3) is implementable as designed. SDK note: parameters are
   `databricks.sdk.service.sql.StatementParameterListItem` objects, and the ambient auth chain
   does not pick up the SP trio from `.env`-style env names — pass
   `azure_client_id/secret/tenant` explicitly (same quirk as the vendored wheel's resolver).
4. **Where does the wheel live and who owns it?** → **dagster-workspace** (decision
   2026-11-12): a new Jira ticket with a feature branch named after it; the library follows the
   repo's `libraries/` conventions (the original port's structure is the template).
5. **Configurator reachability from the runtime environments**: still open — proven from the
   laptop on VPN (§6); one check pending from the target Dagster deployment (containers) and
   from a Databricks bundle. Deployment concern, not a design one.

**Status: the design is buildable.** Remaining before code: the Jira ticket + branch (4), the
runtime reachability check (5), and scaffolding the wheel — everything else is specified.

## 8. Build deltas — what the implementation changed against this brief (2026-11-12)

The wheel is built (`libraries/kaizen-dagster-jobotron` on
`feature/APR-336-kaizen-dagster-jobotron`; 84 tests, ruff + basedpyright 0/0/0, the
repo's pre-commit gates). Four deltas against the brief above, all conservative:

1. **`literal_parameters`** (§4.3's "bind, never interpolate" needed an escape hatch).
   Spark parameter markers cannot sit in every syntactic position: a table name
   (`de.voperdb_{market}`) or an interval literal (`interval {days_countdown} days`)
   is not a value slot. A variable the instance declares in `literal_parameters` is
   substituted as text, but only after the whitelist `^[A-Za-z0-9_]+$` — such a value
   cannot carry SQL syntax, whatever its source: **the whitelist is the injection
   boundary**, not trust in the author. An undeclared variable always binds.
2. **`{market_code}` vs `{com_code}`** (§4.6's topic routing). The notebook's
   `getTopic` routes on `comCode[:2].lower()` (`GRSTX` → `kyc.jobs.gr`) while the
   configurator is asked with the full `comCode`. Two built-ins, two uses:
   `{market_code}` for the topic template, `{com_code}` for the configurator.
3. **Single mode derives `CompanyId` from the rows.** The envelope needs a company id
   and the single-mode config does not state one, so the leak guard (`_one_company`)
   derives it from the result's `CompanyId` column — which every notebook selects
   anyway — and refuses a null, non-int, or mixed batch. In fan-out mode the guard
   additionally requires the rows' company to equal the partition's, so a query that
   lost its `comp_part` filter fails its partition instead of publishing the leak.
4. **Dry-run semantics split by format.** The brief's notebook semantics (publish
   with `IsDryRun: true`, the Worker no-ops) applies to the **envelope** format.
   Plain-JSON messages have no `IsDryRun` field to carry the mode, so they keep the
   sibling wheel's semantics: build everything, send nothing. Documented in the
   README's dry-run section; mirror consumers must skip `IsDryRun: true` records.

Also folded in during the build: the one-source rule takes the *union* of
per-company names before comparing against the other resolvers (the same variable
per company is the point of fan-out, not a conflict), and the configurator's group
code is the sanctioned intermediate — a field the group lookup consumes never needs
to appear in a template.

5. **`include`/`exclude` name companies by id or exact comCode (2026-11-12, post-build).**
   The brief's Alternative sketched `companies: {source: …, include: [7, 1, 4]}` — both
   keys on one block, and ids only. The implementation keeps `include`/`source`
   exclusive, and each list names companies either by id (`7`) or by their **exact
   comCode** (`CYSTX` — compared case-insensitively; never a mix, never a prefix:
   codes share prefixes across markets, `DE` would prune both `DEBTN` and `DEKSN`).
   With comCode entries the partition keys are the codes, `company_id` comes from the
   enumerated row, and — because `{market_code}` (`comCode[:2].lower()`, the
   notebooks' `getTopic`) is a built-in — the demo instance routes its shard and
   topic through it, dropping the brief's per-company `market` parameter entirely
   (`parameters_by_company` keeps only group ids and excluded types).

6. **`parameters_by_company` keys follow the same id-or-comCode rule (2026-11-12, post-build).**
   A key is a company id (`7:`) or an exact comCode (`CYSTX:`, compared
   case-insensitively against the enumerated company), all of one kind per map —
   a mix is refused at load, exactly like include/exclude. The demo instance keys
   it by comCode so the whole fan-out yaml reads in the one convention.

## 9. How to run — the demo walkthrough (2026-11-12, all four variants verified live)

Everything below runs from one terminal on the laptop, **no containers** — the
publish assets talk to Databricks and the STG brokers directly, and the only
Kafka writes go to the demo's own `jobotron_*` topics (the live `kyc.jobs.*`
routing stays commented in every yaml; see the LIVE-TOPIC GUARD headers).

### 9.1 Prerequisites

```bash
devbox shell          # .env: DATABRICKS_*, KAFKA_OUTPUT_*, CONFIGURATOR_BASE_URL;
                      # definitions.py derives MULTI_WORKSPACE_ARM_* and
                      # OPENSHIFT_STG_KAFKA_PASSWORD aliases at import time
```

VPN on (Databricks, the STG Kafka brokers, the configurator). The wheel is
vendored (`vendor/kaizen_dagster_jobotron-0.1.1-py3-none-any.whl`).

### 9.2 Demo topics (one-time)

Ten `jobotron_*` topics on the STG brokers, 1 partition, replication 1 — the
names are the yamls' `topic_template`s with `<market_code>` in `cy/gr/ro`:
`jobotron_unverified_customers_<cc>` (cc = market code here),
`jobotron_{notify_unverified_customers_over_deposit_limit,limit_unverified_customer_rights,verified_customers_with_expired_id}_<cc>`,
`jobotron_kyc_blocked_customers_gr_gr`. Create with `confluent_kafka`'s
`AdminClient` (`NewTopic(name, num_partitions=1, replication_factor=1)`), the
same shape the demo setup asset uses. Topic creation is deliberately not
automated: nothing in the demo should auto-provision Kafka.

### 9.3 `dg dev` and the partition sync

```bash
dg dev                # local webserver + daemon (add -p <port> if :3000 is taken)
```

The **`jobotron_dynamic_partition_sync`** sensor keeps every source-based
instance's dynamic partitions equal to its mcompany enumeration:

1. Open **Automation** and find the `jobotron_dynamic_partition_sync` row —
   the toggle may still be **off** (it was off during the walkthrough); turn
   it on.
2. Wait ~1 minute (the sensor's minimum interval is 60 s) or force it with
   **Evaluate**; check the run log for
   `jobotron_verified_customers_with_expired_id: 28 companies in the enumeration, +28 -0…`.
3. Confirm the codes (CYSTX, GRSTX, ROBTN, CZBTN, PTBTN, DEBTN, …) now appear
   on each source-based instance's Partitions tab.

Static instances (`unverified_customers`' include list,
`kyc_blocked_customers_gr`) need nothing — their partitions exist as soon as
the code loads. Departed companies are removed from the dynamic set only when
no run of theirs is in flight.

### 9.4 Materialize one partition per variant

**Catalog** (the `/assets` page — that is what the left nav calls it in this
Dagster) → the `jobotron` group → `<instance>` → **Partitions** tab → pick the
code → Materialize.
`dry_run: true` is the yaml default; the envelope dry-run **publishes**
`IsDryRun: true` records (the Worker no-ops them) — nothing sends only for
plain-JSON formats. What the 2026-11-12 walkthrough produced:

| variant | instance | partition | outcome |
|---|---|---|---|
| A — static fan-out | `unverified_customers` | CYSTX | 45 envelopes built & published |
| B — configurator | `notify_unverified_customers_over_deposit_limit` | CYSTX, GRSTX | valid skips (`has no isActive`) |
| B | `notify_unverified_customers_over_deposit_limit` | ROBTN | 129 envelopes (only RO is active in the configurator) |
| B′ — + whitelist literal | `limit_unverified_customer_rights` | ROBTN | 24 envelopes (`days_countdown` substituted as text) |
| C — plain fan-out | `verified_customers_with_expired_id` | ROBTN | 108 envelopes |
| D — one-company static | `kyc_blocked_customers_gr` | GRSTX | success, 0 eligible (the sent customers are marked in group 1127) |

A real (non-dry) publish from the LaunchPad needs the folded op key or Dagster
silently ignores the config:
`{"ops": {"jobotron__<instance>": {"config": {"dry_run": false}}}}`.

### 9.5 Verify from Kafka (read-only)

Consume each demo topic from the earliest offset and count — no customer data
is printed, only counts and the envelope's control fields:

```python
from confluent_kafka import Consumer, TopicPartition
import json, os
conf = {"bootstrap.servers": os.environ["KAFKA_OUTPUT_BOOTSTRAP"],
        "group.id": "jobotron-topic-audit", "auto.offset.reset": "earliest",
        "enable.auto.commit": False, "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.environ["KAFKA_OUTPUT_SASL_USERNAME"],
        "sasl.password": os.environ["KAFKA_OUTPUT_SASL_PASSWORD"]}
c = Consumer(conf)
t = "jobotron_notify_unverified_customers_over_deposit_limit_ro"
c.assign([TopicPartition(t, p, 0) for p in c.list_topics(t).topics[t].partitions])
msgs = []
while True:
    batch = c.consume(num_messages=5000, timeout=2.0)
    if not batch: break
    msgs.extend(batch)
vals = [json.loads(m.value()) for m in msgs if not m.error()]
print(len(vals), "message(s),",
      sum(1 for v in vals if v.get("IsDryRun")), "dry-run |",
      "CompanyId:", sorted({v.get("CompanyId") for v in vals}))
```

Expected for the ROBTN run above: `129 message(s), 129 dry-run | CompanyId: [4]`
— the count matches the run's `built N message(s)` line, and the single
`CompanyId` is the leak guard's isolation check passing. The demo consume
asset (containers) mirrors the same topics while skipping the dry-run records.
