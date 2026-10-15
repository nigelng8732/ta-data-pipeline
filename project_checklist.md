# Talent acquisition pipeline checklist

All records are synthetic. Dates are UTC.
snapshot_date identifies an extract, not a business event.
Requisition aging starts at approved_date.
Use closed_date for Closed records and snapshot_date for Open records.
Closure does not necessarily mean an application was Hired.

## 1. Define the source contract
- [ ] Inspect the four CSV schemas and their relationships.
- [ ] Requisition key: job_req_id; recruiter key: recruiter_id.
- [ ] Application key: application_id, not candidate_id.
- [ ] Audit event grain: application_id plus created_date.
- [ ] In this fixture, nonduplicate events have unique timestamps per application.
- [ ] Quarantine conflicting same-time events instead of choosing arbitrarily.

## 2. Snowflake and ingestion
- [ ] Create a separate TA database/project warehouse or reuse small compute.
- [ ] Create RAW, STAGING, MARTS and OPS schemas, stage and CSV format.
- [ ] Retain source values and add file/ingestion metadata.
- [ ] Append snapshots to RAW under immutable dated stage paths.
- [ ] Load all four files as one batch and check expected counts.
- [ ] Record the snapshot as ready only after the complete load succeeds.
- [ ] Test rerunning a snapshot without duplicating its data.

## 3. dbt staging
- [ ] Declare the four RAW sources in YAML.
- [ ] Select one common, latest READY snapshot across all four sources.
- [ ] Do not select a different latest date independently for each table.
- [ ] Parse dates, trim text, validate statuses and deduplicate exact rows.
- [ ] Keep complete audit history from that selected snapshot only.
- [ ] Do not append daily copies of old audit events into the event mart.
- [ ] Create reject/quality models with reasons and source identifiers.
- [ ] Decide and document how invalid parents affect child records.

## 4. Marts
- [ ] dim_recruiters: one row per current recruiter.
- [ ] dim_requisitions: one row per current requisition.
- [ ] dim_candidates: one row per candidate; validate attribute consistency.
- [ ] fct_applications: one row per current application.
- [ ] fct_application_status_events: one row per status-change event.
- [ ] Optionally add a separate daily requisition snapshot fact for aging trends.
- [ ] Keep aging in an appropriate metric model, not as an additive measure.
- [ ] Do not infer historical recruiter ownership from today's assignment.

## 5. Tests and reconciliation
- [ ] Test current-model keys and required fields.
- [ ] Validate recruiter, requisition and application references.
- [ ] Validate exactly the six agreed application statuses.
- [ ] Test approval/application dates and closure chronology.
- [ ] Compare application status with its latest valid audit event.
- [ ] Check source records are accounted for after deduplication/rejection.
- [ ] Load day 1, inspect issues, then load day 2 and verify corrections.
- [ ] Current requisitions stay 20; current applications become 102.
- [ ] Audit mart contains 357 events, not the sum of both daily files.
- [ ] Build, inspect lineage, and add macros only where useful.

## 6. Airflow
- [ ] Container files -> upload -> transactional load -> verify -> dbt/test.
- [ ] Use stored connections; keep credentials out of code and Git.
- [ ] Parameterize snapshot date and expected file counts.
- [ ] Prevent concurrent runs from changing the selected snapshot mid-build.
- [ ] Test first load and same-snapshot rerun before scheduling.
- [ ] State honestly which failure/retry behaviors you have tested.
- [ ] Pause demo schedules when no new input is expected.

## 7. Power BI and portfolio
- [ ] Establish dimensions, fact grain and filter paths.
- [ ] Count applications separately from candidates and audit events.
- [ ] Report open requisitions, applications by status and requisition aging.
- [ ] Use latest status for current funnel counts; audit for movement/history.
- [ ] Show data quality separately from business KPIs.
- [ ] Reconcile the clean second snapshot against expected_results.json.
- [ ] Save architecture, run instructions, screenshots and limitations.
