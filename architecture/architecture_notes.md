# Architecture Notes: Assignment 2.2 (Retail Medallion Architecture)

Flow: **Source files -> Bronze -> Silver -> Gold -> Consumers (aggregates and reports)**.
Object-by-object placement, format, partitioning and load pattern are in `layer_inventory.csv` (45 objects: 17 Bronze, 13 Silver, 15 Gold). All 15 objects from 2.1 appear in Gold exactly once; renamed ones are mapped in `former_2_1_name`.

## 1. Layer contracts

| | Bronze | Silver | Gold |
|---|---|---|---|
| **Purpose** | Land source files exactly as received | Clean, conform and validate | Serve the 2.1 dimensional model |
| **Format** | CSV/PSV file plus all-text Parquet copy | Parquet; rejects in a Snowflake table | Snowflake tables |
| **Partitioning** | `INGEST_DT` | `BUSINESS_DT` / `SNAPSHOT_DT` for facts, none for dimensions | `DAY_KY` for facts, `MONTH_KY` for the aggregate |
| **Guarantees** | Every source row is kept, untouched, with its file name and ingest date. Append-only: nothing is edited or deleted except by a reload. Arrival checked against `EXTRACT_MANIFEST`. | Typed columns, ISO dates, USD in dollars, signs normalised (returns are negative), one location-key form, item keys resolved through UPC, composite sales key, no duplicate keys, parent keys exist. Every dropped row is in `C_REJ_TABLE` with a rule and reason code. | Surrogate keys on every row, SCD1/SCD2 applied, every fact foreign key exists in its dimension, fact grain unique, Gold ties back to Silver. |
| **Does NOT guarantee** | Valid types, valid keys, no duplicates, consistent dates or currency. Defects such as the Xstore trailer mismatch and the repeated NCR header stay in. | History (Silver holds current-state conformed data only), surrogate keys, or business-ready aggregates. | Source-faithful values or the original messy formats. Seeded dimensions (calendar, customer) are not tracked from the source. |
| **Consumers** | Silver only; analysts must not query Bronze | Gold loads only | BI, reports and the aggregate |

**Marts note:** the assignment diagram shows a separate "Marts" box. Here `AGG_SLS_DEPT_DISTRICT_MONTH` is a Gold object (it must sit in exactly one layer), and reports are shown on the diagram as consumers of Gold.

## 2. The architecture departure: Silver conforms all three sales feeds

The reference pattern gives each source its own Silver table. I use **one** `TMP_SALES` built from Xstore, NCR and OMS together.

- **Why:** the hard problems are the same across feeds: composite sales key (ticket numbers restart daily), item key resolution (RMS item, UPC, style or case-pack id), location key forms (bare number, zero-padded, `NL-#####`, `S#####`, `WEB`), signs for returns, currency and money units. Solving them once avoids three copies of the logic and one Gold load instead of three.
- **Trade-off:** a bad feed can delay the shared partition. It is limited by rejecting at row level (with a feed column in `C_REJ_TABLE`) instead of failing the whole table, except at the arrival gate.

## 3. Quality gates

| # | Boundary | Rule | Failure action |
|---|---|---|---|
| G1 | Source -> Bronze | Every file in `EXTRACT_MANIFEST` has arrived, is readable, and its row count matches the manifest | **Fail** (stop before loading) |
| G2 | Bronze -> Silver | Xstore trailer count equals body row count; NCR repeated header rows are detected | **Alert**: the body count is used, the mismatch and the header rows are logged and excluded (known source defects) |
| G3 | Bronze -> Silver | Dates parse (ISO, M/D/YYYY, YYYYMMDD, YYYY/MM/DD, DD-MON-YY, UTC) and quantities and amounts are numeric | **Quarantine** row, reason `BAD_FORMAT` |
| G4 | Bronze -> Silver | Sales key parts, item key and location key are not null | **Quarantine**, reason `NULL_KEY` |
| G5 | Silver | No duplicate composite sales key (store, date, ticket, line, feed) | **Quarantine** later duplicates, keep first, reason `DUPLICATE` |
| G6 | Silver | Item resolves through `TMP_ITEM_UPC` / `TMP_ITEM` and location resolves in `TMP_LOCN` (this also catches voided or unknown items and DC###/case-pack keys) | **Quarantine**, reason `UNRESOLVED_ITEM` / `UNRESOLVED_LOCN` |
| G7 | Silver | Quantity and sign sanity: zero or extreme quantities, negative quantity on a non-return, net amount far from extended minus discount, prices in cents vs dollars | **Quarantine** if impossible, **alert** if only outside tolerance |
| G8 | Silver | Parent keys exist: item -> subclass -> department, district -> region, store -> district | **Quarantine**, reason `ORPHAN_PARENT` |
| G9 | Silver | Sales at a closed store after its close date (store 1118 after 1 Oct 2026) | **Alert** and flag, rows still loaded |
| G10 | Silver | Calendar seed: no repeated day, no missing day | Repeated day: **quarantine** the duplicate. Missing day: **fail** the Gold load (facts cannot get a `DAY_KY`) |
| G11 | Silver -> Gold | Every fact row's item, location and day exist in the Gold dimensions at that date | **Quarantine**, reason `NO_DIM_KEY` |
| G12 | Gold exit | Reconciliation: Bronze rows = Silver rows + rejected rows; Silver net sales = Gold net sales per day | **Fail** (run is marked failed, nothing is published to consumers) |

Reject counts per rule and per day are reported at the end of every run. Nothing is dropped silently.

## 4. Retention

These are assumptions for the project and would be set by the business in practice.

| Layer | Retention | Reason |
|---|---|---|
| Bronze | 24 months, raw files never edited | Needed to replay any day and to prove what the source sent |
| Silver | 90 days of partitions (it can be rebuilt from Bronze) | Cheap to rebuild, only needed for debugging recent loads |
| `C_REJ_TABLE` | 12 months | Reject trends and source feedback |
| Gold | Indefinite | It holds the SCD2 history and cannot be rebuilt from current-state sources alone |

## 5. Reload policy: reprocessing one bad business day D

The unit of reload is a business date. Order is always Silver -> dimensions -> facts -> aggregate -> reconcile, and every step is delete-then-insert on its own partition, so running it twice gives the same counts and totals.

1. **Bronze:** if the source file itself was wrong, land the corrected file again with a new `INGEST_DT`. Otherwise Bronze is untouched. Sales files hold many business dates, so reload does not start at Bronze.
2. **Silver:** delete `TMP_SALES` rows where `BUSINESS_DT = D` (same for `TMP_PRICE`, and `SNAPSHOT_DT = D` for `TMP_INVENTORY`) and `C_REJ_TABLE` rows for that date, then rebuild from Bronze using the latest ingest per source row.
3. **`FCT_SALES`:** delete `DAY_KY` for D and insert from Silver.
4. **`FCT_PRICE` and `FCT_INVENTORY` (positional):** "close then open" means loading D also changed rows from earlier days (the previous row's `EFF_TO_DT` was closed to D-1). Deleting only partition D would leave those wrongly closed. So the reload is: (a) delete rows where `EFF_FROM_DT >= D`; (b) reopen rows that started before D and were closed at or after D-1 (`EFF_TO_DT = 9999-12-31`); (c) replay the Silver snapshots from D forward in date order. This is deterministic because Silver keeps every snapshot for the retention window.
5. **SCD2 dimensions (`DIM_ITEM`, `DIM_LOCN`):** same pattern: delete versions with `EFF_FROM_DT >= D`, reopen the version before it, replay changes from D (for example the store 1112 district move on 15 Sep 2026). SCD1 dimensions are full reloads and need no special handling.
6. **`AGG_SLS_DEPT_DISTRICT_MONTH`:** delete and rebuild the affected `MONTH_KY` from `FCT_SALES`, never increment.
7. **Reconcile (G12)** runs last and fails the run if Gold does not tie to Silver.

Limit: a reload further back than the Silver retention (90 days) must first rebuild Silver from Bronze for the whole range.

## 6. Sources deliberately not landed

The instructor allowed using only the files we need. These are left out of Bronze on purpose:

| Source | Reason |
|---|---|
| `rms/tran_data`, `wms/DC_*` | Daily deltas. `FCT_INVENTORY` is built from full snapshots, and the catalog says snapshots do not tie to summed deltas |
| `rpas/DEMAND_FORECAST_WK` | No forecast fact in the model |
| `pim/STYLE_EXPORT` | Its category tree is not the RMS tree; the model uses the RMS hierarchy |
| `store_ops/LOCATION_ROSTER` | Different codes and region spellings and it drops a store; RMS `STORE` is the authority |
| `rms/CLASS` | The model has department -> subclass only (2.1), so class is skipped |
| `rms/AREA`, `rms/CHAIN` | The model starts at region -> district |
| `rms/WH` | Distribution centres are not locations in the model |
| `rms/PACKITEM` | Case-pack factors only matter for DC stock, which is excluded |

## 7. Known limitations

- Sales keyed by a case-pack id that does not resolve through `ITEM_UPC`, or sold at a `DC###` location, are quarantined (G6) rather than converted.
- The finance seed is a 4-5-4 fiscal calendar (Sunday weeks), but the 2.1 `DIM_MONTH` is a calendar month, so fiscal period reporting is not supported.
- `DIM_CUSTOMER` holds one Unknown row because no source file has customers.
- Retention periods in section 4 are project assumptions.
