# Admin Guide — Exports & User Management

## Roles & Access
- **admin**: full access to all jobs; manages users and exports.
- **verifier**: can review/edit documents only in assigned jobs.
- **viewer**: read-only in assigned jobs.
- Job access is granted via the Access table (UI: Users → assign folders).

## Users
1) Log in as admin.
2) Open **Users**:
   - Fill Email, Password, Role (admin/verifier/viewer).
   - Click **Create user**.
3) Grant job access:
   - In Access, pick the user.
   - Check job folders (prefixes) and save.
   - Job list column “Active verifiers” shows e-mails of verifiers with access.

## Jobs
- Created/edited by admin (customer, name, job_type, priority, folder_prefix).
- Job list hides folders with no data in S3 (bronze/silver) and shows To Review count from live S3.

## Export
- Admin-only.
- In Documents in job:
  - **Export Approved** → `/api/export/batch?job=<prefix>&status=Approved`
  - **Export Rejected** → `/api/export/batch?job=<prefix>&status=Rejected`
- Format: ZIP with `export.csv` (status, filename, dates, amounts, supplier, parsed fields, page_count). Reject reason included if present.
- Via API:
  - Approved: `/api/export/batch?job=PREFIX&status=Approved`
  - Rejected: `/api/export/batch?job=PREFIX&status=Rejected`
- Exports only live JSON+PDF pairs; orphaned records are skipped.

## Document Statuses
- `To Review` (default), `Approved`, `Rejected` (with reason), `Exported`.
- Save/Approve/Reject update JSON in silver; “Exported” is set after export (admin).

## Audit
- Every document update writes an audit file in S3 (`audit/<job>/<timestamp>_<doc>.json`) with user, action, and changed fields.
- Audit is best-effort and never blocks the UI.

## Tips
- If a document doesn’t open and PDF is missing, the portal removes that JSON automatically; re-upload the file.
- If “To Review” shows fewer than “All”, the rest are Approved/Rejected/Exported.
- To update reference CSV: Admin → upload CSV → reload; the `ref` badge appears only on exact matches to the reference.
