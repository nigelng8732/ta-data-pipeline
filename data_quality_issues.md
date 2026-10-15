# Documented source-quality issues

Only the 2026-10-08 snapshot contains deliberate issues.
The 2026-10-09 snapshot corrects all of them.

- R001: exact duplicate recruiter row.
- A001: exact duplicate application row.
- JR003: missing approved_date.
- JR004: unknown recruiter R999.
- JR005: Closed requisition without closed_date.
- A020: unknown requisition JR999.
- A030: missing candidate_id.
- A040: application says Offer; latest audit says Interview.
- A010: exact duplicate of its latest audit event.
- A999: orphan audit event with no application.
- A050: latest audit event has malformed created_date.

Deduplicate exact duplicates. Preserve invalid records for investigation.
Do not invent missing dates, IDs, or status history to make tests pass.
