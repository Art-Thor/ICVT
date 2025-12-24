import json
import os
import re
import copy
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, EmailStr
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    func,
    create_engine,
    text,
    ForeignKey,
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from passlib.context import CryptContext
import jwt
import csv
import io
import zipfile
from ftplib import FTP, FTP_TLS
from fastapi import UploadFile, File


REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket-icvt")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket-icvt")
STATIC_DIR = Path(__file__).parent / "static"
REFERENCE_DIR = Path(__file__).parent / "reference"
REFERENCE_BUCKET = os.getenv("REFERENCE_BUCKET") or SILVER_BUCKET
REFERENCE_PREFIX = os.getenv("REFERENCE_PREFIX", "portal/reference").strip("/")
JOB_CONFIG_BUCKET = os.getenv("JOB_CONFIG_BUCKET") or SILVER_BUCKET
JOB_CONFIG_PREFIX = os.getenv("JOB_CONFIG_PREFIX", "portal/config/jobs").strip("/")
JOB_CONFIG_CACHE_TTL_S = int(os.getenv("JOB_CONFIG_CACHE_TTL_S", "30"))
AUTOMATION_CONFIG_BUCKET = os.getenv("AUTOMATION_CONFIG_BUCKET") or JOB_CONFIG_BUCKET or SILVER_BUCKET
AUTOMATION_CONFIG_KEY = os.getenv("AUTOMATION_CONFIG_KEY", "portal/config/automation.json").strip("/")
AUTOMATION_CACHE_TTL_S = int(os.getenv("AUTOMATION_CACHE_TTL_S", "10"))
EXPORT_FTP_HOST = os.getenv("EXPORT_FTP_HOST")
EXPORT_FTP_USER = os.getenv("EXPORT_FTP_USER")
EXPORT_FTP_PASSWORD = os.getenv("EXPORT_FTP_PASSWORD")
EXPORT_FTP_DIR = os.getenv("EXPORT_FTP_DIR", "").strip("/")
EXPORT_FTP_TLS = os.getenv("EXPORT_FTP_TLS", "0").lower() in ("1", "true", "yes")
REFERENCE_FTP_HOST = os.getenv("REFERENCE_FTP_HOST")
REFERENCE_FTP_USER = os.getenv("REFERENCE_FTP_USER")
REFERENCE_FTP_PASSWORD = os.getenv("REFERENCE_FTP_PASSWORD")
REFERENCE_FTP_DIR = os.getenv("REFERENCE_FTP_DIR", "").strip("/")
REFERENCE_FTP_TLS = os.getenv("REFERENCE_FTP_TLS", "0").lower() in ("1", "true", "yes")
REFERENCE_FTP_FILES = [s.strip() for s in os.getenv("REFERENCE_FTP_FILES", "").split(",") if s.strip()]
DB_URL = os.getenv("DB_URL", f"sqlite:///{Path(__file__).parent / 'portal.db'}")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-secret")
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "24"))
INVOICES_TABLE = os.getenv("INVOICES_TABLE", "invoice-portal-invoices")
AUDIT_TABLE = os.getenv("AUDIT_TABLE", "invoice-portal-audit")
AUDIT_BUCKET = os.getenv("AUDIT_BUCKET")  # optional, else use SILVER_BUCKET

S3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
pwd_context = CryptContext(
    # pbkdf2_sha256 avoids the 72-byte bcrypt limit; suitable for the POC/local portal
    schemes=["pbkdf2_sha256"],
    deprecated="auto",
)

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
TS_PREFIX_RE = re.compile(r"^(\d{10,13})")
SAFE_FOLDER_RE = re.compile(r"^[A-Za-z0-9._/+\\-]*$")
REFERENCE_DATA = {
    "suppliers": [],
    "po": [],
    "invoices": [],
    "indexes": {},
}

_JOB_CONFIG_CACHE = {}  # job_prefix -> {"ts": float, "config": dict}
_AUTOMATION_CACHE = {"ts": 0.0, "config": None}


def sanitize_folder(folder: Optional[str]) -> str:
    if not folder:
        return ""
    if not SAFE_FOLDER_RE.match(folder):
        raise HTTPException(status_code=400, detail="Invalid folder name")
    return folder.strip("/")


def extract_date_parts_from_filename(filename: str):
    # 1) try timestamp prefix (10 or 13 digits)
    ts_match = TS_PREFIX_RE.match(filename)
    if ts_match:
        raw_ts = ts_match.group(1)
        try:
            ts_int = int(raw_ts)
            if len(raw_ts) == 13:
                ts_int = ts_int / 1000.0
            dt = datetime.utcfromtimestamp(ts_int)
            return dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        except Exception:
            pass

    # 2) fallback to first YYYY-MM-DD in name
    match = DATE_RE.search(filename)
    if not match:
        return "unknown", "unknown", "unknown"
    return match.group(1), match.group(2), match.group(3)


def build_dated_prefix(folder: str, filename: str):
    year, month, day = extract_date_parts_from_filename(filename)
    parts: List[str] = []
    if folder:
        parts.append(folder.strip("/"))
    parts.extend([year, month, day])
    return "/".join(filter(None, parts))


def build_key(folder: str, filename: str) -> str:
    folder = sanitize_folder(folder)
    return f"{folder}/{filename}" if folder else filename


def object_exists(bucket: str, key: str) -> bool:
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def load_reference_tables():
    REFERENCE_DATA["suppliers"].clear()
    REFERENCE_DATA["po"].clear()
    REFERENCE_DATA["invoices"].clear()
    REFERENCE_DATA["indexes"] = {
        "by_id": {},
        "by_number": {},
        "by_name": set(),
        "suppliers_by_name": {},
        "po_by_number": {},
        "suppliers_by_gst_abn": {},
    }

    def load_csv_filelike(file_obj):
        rows = []
        reader = csv.DictReader((line.decode("utf-8", errors="ignore") for line in file_obj.iter_lines()))
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
        return rows

    def load_csv_local(path: Path):
        if not path.exists():
            return []
        rows = []
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
        return rows

    def load_csv_named(filename: str):
        # Prefer S3 (so it can be updated without redeploy); fallback to local repo file.
        if REFERENCE_BUCKET and REFERENCE_PREFIX:
            s3_key = f"{REFERENCE_PREFIX}/{filename}"
        elif REFERENCE_BUCKET:
            s3_key = filename
        else:
            s3_key = None

        if REFERENCE_BUCKET and s3_key:
            try:
                obj = S3.get_object(Bucket=REFERENCE_BUCKET, Key=s3_key)
                body = obj["Body"]
                return load_csv_filelike(body)
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                    raise

        return load_csv_local(REFERENCE_DIR / filename)

    jar_sup_name = "JAR Supplier Details.csv"
    jar_sup = load_csv_named(jar_sup_name)
    if jar_sup:
        REFERENCE_DATA["suppliers"].extend(
            [
                {
                    "source": "jar",
                    "customer_id": r.get("Cre Account", ""),
                    "name": r.get("Cre Name", ""),
                    "gst_abn": "",
                    "currency": "",
                }
                for r in jar_sup
            ]
        )

    nzme_sup_name = "NZME JDE Supplier Details.csv"
    nzme_sup = load_csv_named(nzme_sup_name)
    if nzme_sup:
        REFERENCE_DATA["suppliers"].extend(
            [
                {
                    "source": "nzme",
                    "customer_id": r.get("Number", ""),
                    "name": r.get("Name", ""),
                    "gst_abn": r.get("GST_ABN", ""),
                    "currency": r.get("Currency", ""),
                }
                for r in nzme_sup
            ]
        )

    jar_po_name = "JAR Database Table PO Details.csv"
    jar_po = load_csv_named(jar_po_name)
    if jar_po:
        REFERENCE_DATA["po"].extend(jar_po)

    jar_inv_name = "JAR Invoice Details.csv"
    jar_inv = load_csv_named(jar_inv_name)
    if jar_inv:
        REFERENCE_DATA["invoices"].extend(jar_inv)

    # build indexes
    for sup in REFERENCE_DATA["suppliers"]:
        cid = (sup.get("customer_id") or "").strip()
        if cid:
            REFERENCE_DATA["indexes"].setdefault("by_id", {}).setdefault(cid.upper(), []).append(sup)
        name = (sup.get("name") or "").strip()
        if name:
            name_up = name.upper()
            REFERENCE_DATA["indexes"]["by_name"].add(name_up)
            REFERENCE_DATA["indexes"].setdefault("suppliers_by_name", {}).setdefault(name_up, []).append(sup)
        gst_abn = (sup.get("gst_abn") or "").strip()
        if gst_abn:
            norm = re.sub(r"[^0-9A-Za-z]+", "", gst_abn).upper()
            if norm:
                REFERENCE_DATA["indexes"].setdefault("suppliers_by_gst_abn", {}).setdefault(norm, []).append(sup)

    for po in REFERENCE_DATA["po"]:
        po_no = (po.get("PO No") or po.get("PO") or "").strip()
        if not po_no:
            continue
        REFERENCE_DATA["indexes"].setdefault("po_by_number", {}).setdefault(po_no.upper(), []).append(po)
    for inv in REFERENCE_DATA["invoices"]:
        cid = (inv.get("Cre Account") or "").strip()
        inv_no = (inv.get("Invoice No") or "").strip()
        if cid:
            REFERENCE_DATA["indexes"].setdefault("by_id", {}).setdefault(cid.upper(), []).append(inv)
        if inv_no:
            REFERENCE_DATA["indexes"].setdefault("by_number", {}).setdefault(inv_no.upper(), []).append(inv)



def presign_put(bucket: str, key: str, content_type: str, expires: int = 900) -> str:
    return S3.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key, "ContentType": content_type},
        ExpiresIn=expires,
    )


def presign_get(bucket: str, key: str, expires: int = 900) -> str:
    return S3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires)


def candidate_textract_keys(folder: str, filename: str) -> List[str]:
    base = os.path.splitext(os.path.basename(filename))[0]
    dated_prefix = build_dated_prefix(folder, base)
    keys = [f"textract-results/{dated_prefix}/{base}.json"]

    # job-based path: textract-results/<job-folder>/...
    if folder:
        job_key = f"textract-results/{folder}/{base}.json"
        if job_key not in keys:
            keys.append(job_key)

    # legacy without a prefix
    legacy = f"textract-results/{base}.json"
    if legacy not in keys:
        keys.append(legacy)

    return keys


def candidate_export_keys(folder: str, filename: str) -> List[str]:
    base = os.path.splitext(os.path.basename(filename))[0] + ".zip"
    dated_prefix = build_dated_prefix(folder, filename)
    keys = [f"export/{dated_prefix}/{base}"]
    if folder and f"export/{folder}/{base}" not in keys:
        keys.append(f"export/{folder}/{base}")
    return keys


def parse_job_doc_from_key(key: str):
    """
    Expect textract-results/<job_prefix>/<file>.json -> return (job_prefix, doc_id)
    """
    if not key.startswith("textract-results/"):
        return None, None
    parts = key.split("/", 2)
    if len(parts) < 3:
        return None, None
    job_prefix = parts[1]
    doc_id = os.path.basename(key)
    return job_prefix, doc_id


def stream_csv_zip(job_prefix: str, rows: List[dict]) -> StreamingResponse:
    output = io.BytesIO()
    export_csv = build_export_csv_bytes(job_prefix, rows)
    meta_csv = build_meta_csv_bytes(rows)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("export.csv", export_csv)
        zf.writestr("meta.csv", meta_csv)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="export_{job_prefix}.zip"'},
    )


def list_pending_counts(prefixes: List[str], max_scan: int = 5000) -> dict:
    """
    Returns {prefix: count_pending}, counting JSON in textract-results/<prefix> whose status is To Review (or missing),
    and only if the source PDF exists. Iterates through all pages (up to max_scan per prefix) for accuracy.
    """
    counts = {}
    for prefix in prefixes:
        total = 0
        # S3 only
        s3_prefix = f"textract-results/{prefix.strip('/')}/"
        paginator = S3.get_paginator("list_objects_v2")
        scanned = 0
        for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                if scanned >= max_scan:
                    break
                key = obj["Key"]
                if not key.lower().endswith(".json"):
                    continue
                scanned += 1
                try:
                    data = S3.get_object(Bucket=SILVER_BUCKET, Key=key)
                    payload = json.loads(data["Body"].read())
                    status = str(payload.get("status", "")).lower()
                    if status not in ("", "to review"):
                        continue
                    if not source_exists(payload):
                        try:
                            S3.delete_object(Bucket=SILVER_BUCKET, Key=key)
                        except Exception:
                            pass
                        continue
                    payload["status"] = canonical_status(payload.get("status"))
                    total += 1
                except Exception:
                    continue
            if scanned >= max_scan:
                break
        counts[prefix] = total
    return counts


def load_json_from_s3(key: str) -> dict:
    obj = S3.get_object(Bucket=SILVER_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def save_json_to_s3(key: str, payload: dict):
    S3.put_object(
        Bucket=SILVER_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )


def canonical_status(val: Optional[str]) -> str:
    s = (val or "").strip().lower()
    if s == "approved":
        return "Approved"
    if s == "rejected":
        return "Rejected"
    if s == "exported":
        return "Exported"
    if s in ("hold", "on hold", "on-hold", "on_hold", "h"):
        return "Hold"
    return "To Review"


def parse_payload_datetime(val) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, datetime):
        dt = val
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    raw = str(val).strip()
    if not raw:
        return None
    # Support ISO formats and "Z" suffix.
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def job_config_s3_key(job_prefix: str) -> str:
    base = JOB_CONFIG_PREFIX.strip("/")
    job_prefix = job_prefix.strip("/")
    if base:
        return f"{base}/{job_prefix}.json"
    return f"{job_prefix}.json"


def get_job_config(job_prefix: str) -> dict:
    job_prefix = sanitize_folder(job_prefix)
    if not job_prefix:
        return {}

    cached = _JOB_CONFIG_CACHE.get(job_prefix)
    now = time.time()
    if cached and (now - cached.get("ts", 0) < JOB_CONFIG_CACHE_TTL_S):
        return cached.get("config") or {}

    key = job_config_s3_key(job_prefix)
    try:
        obj = S3.get_object(Bucket=JOB_CONFIG_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        if not isinstance(data, dict):
            data = {}
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            data = {}
        else:
            raise
    _JOB_CONFIG_CACHE[job_prefix] = {"ts": now, "config": data}
    return data


def save_job_config(job_prefix: str, config: dict):
    job_prefix = sanitize_folder(job_prefix)
    if not job_prefix:
        raise HTTPException(status_code=400, detail="job is required")
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    key = job_config_s3_key(job_prefix)
    S3.put_object(
        Bucket=JOB_CONFIG_BUCKET,
        Key=key,
        Body=json.dumps(config, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    _JOB_CONFIG_CACHE.pop(job_prefix, None)


def get_automation_config() -> dict:
    now = time.time()
    cached = _AUTOMATION_CACHE.get("config")
    if cached is not None and (now - float(_AUTOMATION_CACHE.get("ts") or 0) < AUTOMATION_CACHE_TTL_S):
        return cached

    cfg: dict = {"enabled": True}
    if AUTOMATION_CONFIG_BUCKET and AUTOMATION_CONFIG_KEY:
        try:
            obj = S3.get_object(Bucket=AUTOMATION_CONFIG_BUCKET, Key=AUTOMATION_CONFIG_KEY)
            data = json.loads(obj["Body"].read())
            if isinstance(data, dict):
                cfg.update(data)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise

    cfg["enabled"] = bool(cfg.get("enabled", True))
    _AUTOMATION_CACHE["ts"] = now
    _AUTOMATION_CACHE["config"] = cfg
    return cfg


def is_automation_enabled() -> bool:
    return bool(get_automation_config().get("enabled", True))


def save_automation_config(enabled: bool, updated_by: str):
    if not (AUTOMATION_CONFIG_BUCKET and AUTOMATION_CONFIG_KEY):
        raise HTTPException(status_code=500, detail="Automation config storage is not configured")
    cfg = {
        "enabled": bool(enabled),
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "updated_by": updated_by,
    }
    S3.put_object(
        Bucket=AUTOMATION_CONFIG_BUCKET,
        Key=AUTOMATION_CONFIG_KEY,
        Body=json.dumps(cfg, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    _AUTOMATION_CACHE["ts"] = time.time()
    _AUTOMATION_CACHE["config"] = cfg


def _normalized_field_map(payload: dict) -> dict:
    out = {}
    for f in payload.get("normalized_data") or []:
        k = (f.get("key") or f.get("field_name") or "").strip()
        if not k:
            continue
        out[k.lower()] = f
    return out


def _try_parse_date(val: str) -> Optional[str]:
    raw = (val or "").strip()
    if not raw:
        return None
    # Common invoice date formats (NZ/AU + ISO)
    candidates = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in candidates:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    return None


def _try_parse_amount(val: str) -> Optional[float]:
    raw = (val or "").strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^0-9.,-]+", "", raw)
    if cleaned.count(",") > 0 and cleaned.count(".") == 0:
        # "1,23" -> "1.23"
        cleaned = cleaned.replace(",", ".")
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except Exception:
        return None


def validate_document(payload: dict, job_prefix: str) -> List[dict]:
    cfg = get_job_config(job_prefix) or {}
    fields = _normalized_field_map(payload)

    required_fields = cfg.get("required_fields")
    if not isinstance(required_fields, list):
        if job_prefix.upper().startswith("JAR"):
            required_fields = ["Invoice Number", "Invoice Date", "Total Amount", "Supplier/Vendor Name"]
        else:
            required_fields = ["Invoice Number", "Invoice Date"]

    date_fields = cfg.get("date_fields")
    if not isinstance(date_fields, list):
        date_fields = ["Invoice Date"]

    amount_fields = cfg.get("amount_fields")
    if not isinstance(amount_fields, list):
        amount_fields = ["Total Amount", "GST Amount", "Invoice Total"]

    errors: List[dict] = []

    for name in required_fields:
        f = fields.get(str(name).strip().lower())
        val = (f or {}).get("value")
        if not (val or "").strip():
            errors.append({"field": name, "error": "required"})

    for name in date_fields:
        f = fields.get(str(name).strip().lower())
        val = (f or {}).get("value")
        if (val or "").strip() and not _try_parse_date(val):
            errors.append({"field": name, "error": "invalid_date"})

    for name in amount_fields:
        f = fields.get(str(name).strip().lower())
        val = (f or {}).get("value")
        if (val or "").strip() and _try_parse_amount(val) is None:
            errors.append({"field": name, "error": "invalid_amount"})

    return errors


def _norm_value(payload: dict, *names: str) -> str:
    fields = _normalized_field_map(payload)
    for name in names:
        k = (name or "").strip().lower()
        if not k:
            continue
        val = (fields.get(k) or {}).get("value")
        if (val or "").strip():
            return str(val).strip()
    return ""


def _jar_creditor_from_reference(payload: dict) -> tuple[str, str]:
    po = _norm_value(payload, "PO Number", "PO No", "PO")
    if po:
        rows = REFERENCE_DATA["indexes"].get("po_by_number", {}).get(po.upper()) or []
        if rows:
            return (rows[0].get("Cre Account") or "").strip(), (rows[0].get("Cre Name") or "").strip()

    name = _norm_value(payload, "Supplier/Vendor Name", "Creditor Name", "Supplier", "Vendor")
    if name:
        rows = REFERENCE_DATA["indexes"].get("suppliers_by_name", {}).get(name.upper()) or []
        if rows:
            return (rows[0].get("customer_id") or "").strip(), (rows[0].get("name") or "").strip()

    return "", ""


def _export_schema_from_config(job_prefix: str) -> Optional[List[dict]]:
    cfg = get_job_config(job_prefix) or {}
    schema = cfg.get("export_schema") or cfg.get("export", {}).get("schema")
    return schema if isinstance(schema, list) else None


def _build_csv_from_schema(schema: List[dict], payloads: List[dict]) -> bytes:
    # schema: [{column, field|source|value, type?}] ; payloads: list of document payload dicts
    output = io.StringIO()
    writer = csv.writer(output)
    cols = [str(c.get("column") or "").strip() for c in schema]
    writer.writerow(cols)

    for p in payloads:
        row = []
        for c in schema:
            val = ""
            if "value" in c:
                val = c.get("value")
            elif c.get("source"):
                val = p.get(str(c.get("source")))
            elif c.get("field"):
                val = _norm_value(p, str(c.get("field")))
            t = str(c.get("type") or "").strip().lower()
            if t == "date":
                parsed = _try_parse_date(str(val or ""))
                val = parsed or (val or "")
            elif t == "amount":
                parsed = _try_parse_amount(str(val or ""))
                val = "" if parsed is None else f"{parsed:.2f}"
            row.append(val if val is not None else "")
        writer.writerow(row)
    return output.getvalue().encode("utf-8")


def build_export_csv_bytes(job_prefix: str, payloads: List[dict]) -> bytes:
    schema = _export_schema_from_config(job_prefix)
    if schema:
        return _build_csv_from_schema(schema, payloads)

    # Defaults per job type (fallbacks until job config is defined).
    if job_prefix.upper().startswith("JAR"):
        header = ["Cre Account", "Cre Name", "PO No", "Invoice No", "Invoice total", "Sub Total", "Invoice Date", "Uploaded Date"]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(header)
        for p in payloads:
            cre_account, cre_name = _jar_creditor_from_reference(p)
            po_no = _norm_value(p, "PO Number", "PO No", "PO")
            inv_no = _norm_value(p, "Invoice Number", "Invoice No")
            inv_total = _norm_value(p, "Invoice Total", "Total Amount", "Invoice total")
            inv_date = _norm_value(p, "Invoice Date")
            inv_date = _try_parse_date(inv_date) or inv_date
            uploaded = p.get("processed_at") or p.get("updated_at") or ""
            if isinstance(uploaded, datetime):
                uploaded = uploaded.isoformat()
            uploaded = str(uploaded or "")
            writer.writerow([cre_account, cre_name, po_no, inv_no, inv_total, "", inv_date, uploaded])
        return output.getvalue().encode("utf-8")

    # Generic CSV for non-JAR jobs
    header = ["status", "reason", "created_at", "invoice_number", "invoice_date", "total_amount", "gst_amount", "supplier", "po_number", "account_number", "key", "page_count"]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for p in payloads:
        fields_map = _normalized_field_map(p)

        def val(name: str):
            return (fields_map.get(name.lower()) or {}).get("value") or ""

        writer.writerow([
            canonical_status(p.get("status")),
            p.get("reason") or "",
            p.get("processed_at") or "",
            val("Invoice Number"),
            _try_parse_date(val("Invoice Date")) or val("Invoice Date"),
            val("Total Amount") or val("Invoice Total"),
            val("GST Amount"),
            val("Supplier/Vendor Name") or val("Creditor Name"),
            val("PO Number"),
            val("Account Number"),
            p.get("key") or "",
            p.get("page_count") or p.get("pages") or "",
        ])
    return output.getvalue().encode("utf-8")


def build_kv_csv_bytes(payload: dict) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["field", "value", "confidence"])
    for f in payload.get("normalized_data") or []:
        writer.writerow([f.get("key") or f.get("field_name") or "", f.get("value") or "", f.get("confidence") or f.get("gptConfidence") or ""])
    return output.getvalue().encode("utf-8")


def build_meta_csv_bytes(payloads: List[dict]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["key", "status", "reason", "processed_at", "updated_at", "export_key"])
    for p in payloads:
        writer.writerow([
            p.get("key") or "",
            canonical_status(p.get("status")),
            p.get("reason") or "",
            p.get("processed_at") or "",
            p.get("updated_at") or "",
            p.get("export_key") or "",
        ])
    return output.getvalue().encode("utf-8")


def build_export_zip_bytes(job_prefix: str, payload: dict, source_filename: str, source_bytes: bytes) -> bytes:
    cfg = get_job_config(job_prefix) or {}
    extra = cfg.get("extra_exports") if isinstance(cfg.get("extra_exports"), list) else []

    export_csv = build_export_csv_bytes(job_prefix, [payload])
    kv_csv = build_kv_csv_bytes(payload)

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(source_filename, source_bytes)
        zf.writestr("export.csv", export_csv)
        zf.writestr("kv.csv", kv_csv)
        for item in extra:
            if not isinstance(item, dict):
                continue
            filename = (item.get("filename") or "").strip() or "extra.csv"
            schema = item.get("schema")
            if isinstance(schema, list):
                zf.writestr(filename, _build_csv_from_schema(schema, [payload]))
    output.seek(0)
    return output.getvalue()


def _ftp_upload_bytes(remote_path: str, data: bytes):
    if not (EXPORT_FTP_HOST and EXPORT_FTP_USER and EXPORT_FTP_PASSWORD):
        return
    remote_path = (remote_path or "").lstrip("/")
    parts = [p for p in remote_path.split("/") if p]
    if not parts:
        raise ValueError("remote_path required")
    filename = parts[-1]
    dirs = parts[:-1]

    ftp = FTP_TLS(EXPORT_FTP_HOST) if EXPORT_FTP_TLS else FTP(EXPORT_FTP_HOST)
    ftp.login(EXPORT_FTP_USER, EXPORT_FTP_PASSWORD)
    if EXPORT_FTP_TLS and isinstance(ftp, FTP_TLS):
        ftp.prot_p()

    def ensure_dir(path: str):
        if not path:
            return
        try:
            ftp.cwd(path)
        except Exception:
            ftp.mkd(path)
            ftp.cwd(path)

    if EXPORT_FTP_DIR:
        ensure_dir(EXPORT_FTP_DIR)
    for d in dirs:
        ensure_dir(d)

    bio = io.BytesIO(data)
    ftp.storbinary(f"STOR {filename}", bio)
    try:
        ftp.quit()
    except Exception:
        ftp.close()


def _ftp_download_bytes(remote_path: str) -> bytes:
    if not (REFERENCE_FTP_HOST and REFERENCE_FTP_USER and REFERENCE_FTP_PASSWORD):
        raise RuntimeError("Reference FTP is not configured")
    remote_path = (remote_path or "").lstrip("/")
    parts = [p for p in remote_path.split("/") if p]
    if not parts:
        raise ValueError("remote_path required")
    filename = parts[-1]
    dirs = parts[:-1]

    ftp = FTP_TLS(REFERENCE_FTP_HOST) if REFERENCE_FTP_TLS else FTP(REFERENCE_FTP_HOST)
    ftp.login(REFERENCE_FTP_USER, REFERENCE_FTP_PASSWORD)
    if REFERENCE_FTP_TLS and isinstance(ftp, FTP_TLS):
        ftp.prot_p()

    def cwd_safe(path: str):
        if not path:
            return
        ftp.cwd(path)

    if REFERENCE_FTP_DIR:
        cwd_safe(REFERENCE_FTP_DIR)
    for d in dirs:
        cwd_safe(d)

    out = io.BytesIO()

    def _writer(chunk: bytes):
        out.write(chunk)

    ftp.retrbinary(f"RETR {filename}", _writer)
    try:
        ftp.quit()
    except Exception:
        ftp.close()
    return out.getvalue()

def apply_reference_validation(normalized: List[dict]) -> List[dict]:
    by_id = REFERENCE_DATA["indexes"].get("by_id", {})
    by_number = REFERENCE_DATA["indexes"].get("by_number", {})
    by_name = REFERENCE_DATA["indexes"].get("by_name", set())
    by_gst = REFERENCE_DATA["indexes"].get("suppliers_by_gst_abn", {})
    out = []
    for item in normalized:
        key = (item.get("key") or item.get("field_name") or "").strip()
        val = (item.get("value") or "").strip()
        status = None
        if val:
            v_up = val.upper()
            k_low = key.lower()
            if "invoice number" in k_low or "invoice #" in k_low:
                if v_up in by_number:
                    status = "match"
            if "customer" in k_low or "account" in k_low or "id" in k_low:
                if v_up in by_id:
                    status = "match"
            if "supplier" in k_low or "vendor" in k_low or "creditor" in k_low:
                if v_up in by_name:
                    status = "match"
            if "gst" in k_low or "abn" in k_low:
                norm = re.sub(r"[^0-9A-Za-z]+", "", val).upper()
                if norm and norm in by_gst:
                    status = "match"
        if status != "match":
            status = ""
        item = dict(item)
        item["validation"] = {"status": status}
        out.append(item)
    return out


def _lookup_status(count: int) -> str:
    if count <= 0:
        return "no_match"
    if count == 1:
        return "match"
    return "ambiguous"


def _lookup_result(candidates: List[dict]) -> dict:
    status = _lookup_status(len(candidates))
    return {
        "status": status,
        "match_count": len(candidates),
        "candidates": candidates,
        "selected": candidates[0] if status == "match" else None,
    }


def _supplier_data_from_row(row: dict) -> dict:
    return {
        "customer_id": (row.get("customer_id") or row.get("Cre Account") or row.get("Number") or row.get("Customer ID") or "").strip(),
        "name": (row.get("name") or row.get("Cre Name") or row.get("Name") or row.get("Supplier") or "").strip(),
        "gst_abn": (row.get("gst_abn") or row.get("GST_ABN") or row.get("GST/ABN") or "").strip(),
        "currency": (row.get("currency") or row.get("Currency") or "").strip(),
        "source": row.get("source") or row.get("Source") or "reference",
    }


def _po_data_from_row(row: dict, po_number: str) -> dict:
    return {
        "po_number": (row.get("PO No") or row.get("PO") or po_number or "").strip(),
        "customer_id": (row.get("Cre Account") or row.get("customer_id") or "").strip(),
        "name": (row.get("Cre Name") or row.get("name") or "").strip(),
    }


def run_unified_lookup(payload: dict, job_prefix: str) -> dict:
    """
    Best-effort unified lookup engine.
    Uses in-memory reference CSVs (loaded from S3 or local) + simple matching rules.
    """
    cfg = get_job_config(job_prefix) or {}
    lookup_cfg = cfg.get("lookup") if isinstance(cfg.get("lookup"), dict) else {}

    po_number = _norm_value(payload, "PO Number", "PO No", "PO")
    supplier_name = _norm_value(payload, "Supplier/Vendor Name", "Supplier Name", "Supplier", "Vendor", "Creditor Name", "Creditor")
    gst_abn = _norm_value(payload, "GST/ABN", "GST", "ABN", "GST Number", "ABN Number")
    invoice_number = _norm_value(payload, "Invoice Number", "Invoice No", "Invoice #")
    customer_id = _norm_value(payload, "Customer ID", "Customer", "Account Code", "Creditor Code", "Cre Account", "Supplier Code", "Vendor Code")

    po_candidates: List[dict] = []
    if po_number:
        rows = REFERENCE_DATA["indexes"].get("po_by_number", {}).get(po_number.upper()) or []
        for r in rows:
            data = _po_data_from_row(r, po_number)
            po_candidates.append(
                {
                    "id": data.get("po_number") or po_number,
                    "confidence": 1.0,
                    "matched_on": ["po_number"],
                    "data": data,
                }
            )

    supplier_candidates_by_id: Dict[str, dict] = {}

    def upsert_supplier(row: dict, matched_on: str, confidence: float):
        data = _supplier_data_from_row(row)
        if not data.get("customer_id") and not data.get("name"):
            return
        cid = data.get("customer_id") or data.get("name")
        key = (cid or "").upper()
        if not key:
            return
        existing = supplier_candidates_by_id.get(key)
        if existing and float(existing.get("confidence") or 0) >= confidence:
            return
        supplier_candidates_by_id[key] = {
            "id": cid,
            "confidence": confidence,
            "matched_on": [matched_on],
            "data": data,
        }

    # 1) PO match can derive supplier
    if po_candidates:
        for c in po_candidates:
            po_data = (c.get("data") or {})
            if po_data.get("customer_id") or po_data.get("name"):
                upsert_supplier(po_data, "po_number", float(c.get("confidence") or 1.0))

    # 2) GST/ABN match
    if gst_abn:
        norm = re.sub(r"[^0-9A-Za-z]+", "", gst_abn).upper()
        rows = REFERENCE_DATA["indexes"].get("suppliers_by_gst_abn", {}).get(norm) or []
        for r in rows:
            upsert_supplier(r, "gst_abn", 1.0)

    # 3) Customer ID match
    if customer_id:
        rows = REFERENCE_DATA["indexes"].get("by_id", {}).get(customer_id.upper()) or []
        for r in rows:
            upsert_supplier(r, "customer_id", 1.0)

    # 4) Supplier name match (exact)
    if supplier_name:
        rows = REFERENCE_DATA["indexes"].get("suppliers_by_name", {}).get(supplier_name.upper()) or []
        for r in rows:
            upsert_supplier(r, "supplier_name", 1.0)

    supplier_candidates = list(supplier_candidates_by_id.values())
    supplier_candidates.sort(key=lambda c: float(c.get("confidence") or 0), reverse=True)

    invoice_candidates: List[dict] = []
    if invoice_number:
        rows = REFERENCE_DATA["indexes"].get("by_number", {}).get(invoice_number.upper()) or []
        for r in rows:
            inv_no = (r.get("Invoice No") or r.get("invoice_number") or invoice_number or "").strip()
            cid = (r.get("Cre Account") or r.get("customer_id") or "").strip()
            invoice_candidates.append(
                {
                    "id": inv_no or invoice_number,
                    "confidence": 1.0,
                    "matched_on": ["invoice_number"],
                    "data": {"invoice_number": inv_no or invoice_number, "customer_id": cid},
                }
            )

    supplier_res = _lookup_result(supplier_candidates)
    po_res = _lookup_result(po_candidates)
    invoice_res = _lookup_result(invoice_candidates)

    # Suggested autofill: safe defaults (client should apply only if field is empty and unlocked).
    suggested_fields: List[dict] = []
    selected_supplier = (supplier_res.get("selected") or {}).get("data") if supplier_res.get("status") == "match" else None
    selected_po = (po_res.get("selected") or {}).get("data") if po_res.get("status") == "match" else None

    def suggest(field_name: str, value: str, source: str, confidence: float = 1.0):
        v = (value or "").strip()
        if not v:
            return
        suggested_fields.append({"field": field_name, "value": v, "source": source, "confidence": confidence})

    if selected_po:
        suggest("Supplier/Vendor Name", selected_po.get("name") or "", "po")
        suggest("Creditor Name", selected_po.get("name") or "", "po")
    if selected_supplier:
        suggest("Supplier/Vendor Name", selected_supplier.get("name") or "", "supplier")
        suggest("Creditor Name", selected_supplier.get("name") or "", "supplier")
        suggest("GST/ABN", selected_supplier.get("gst_abn") or "", "supplier")

    # Optional config-driven extra suggestions
    extra_suggestions = lookup_cfg.get("suggest") if isinstance(lookup_cfg.get("suggest"), list) else []
    for item in extra_suggestions:
        if not isinstance(item, dict):
            continue
        field = (item.get("field") or "").strip()
        value = (item.get("value") or "").strip()
        source = (item.get("source") or "config").strip()
        if field and value:
            try:
                conf = float(item.get("confidence") or 1.0)
            except Exception:
                conf = 1.0
            suggest(field, value, source, conf)

    return {
        "job": job_prefix,
        "inputs": {
            "po_number": po_number,
            "supplier_name": supplier_name,
            "gst_abn": gst_abn,
            "invoice_number": invoice_number,
            "customer_id": customer_id,
        },
        "supplier": supplier_res,
        "po": po_res,
        "invoice": invoice_res,
        "suggested_fields": suggested_fields,
    }


def is_key_under_job(key: str, job_prefix: str) -> bool:
    job_prefix = job_prefix.strip("/")
    expected_prefix = f"textract-results/{job_prefix}/"
    return key.startswith(expected_prefix)


def write_audit(event: dict):
    """
    Persist a small audit record to S3 (AUDIT_BUCKET or SILVER_BUCKET) under audit/<job>/<ts>_<doc>.json
    """
    bucket = AUDIT_BUCKET or SILVER_BUCKET
    job = event.get("job_prefix") or "unknown"
    doc = event.get("doc_id") or "unknown"
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key = f"audit/{job}/{ts}_{doc}.json"
    try:
        S3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(event, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )
    except Exception:
        # avoid failing main request due to audit
        pass


def source_exists(payload: dict) -> bool:
    bucket = payload.get("source_bucket") or BRONZE_BUCKET
    key = payload.get("source_file") or payload.get("source_key")
    if not key:
        return False
    try:
        return object_exists(bucket, key)
    except Exception:
        return False


def prefix_has_data(prefix: str) -> bool:
    """Check if there is at least one object under textract-results/<prefix>/ or raw uploads under <prefix>/ in bronze."""
    pref = prefix.strip("/")
    try:
        resp = S3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=f"textract-results/{pref}/", MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            return True
    except Exception:
        pass
    try:
        resp = S3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=f"{pref}/", MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            return True
    except Exception:
        pass
    return False


class UploadUrlRequest(BaseModel):
    filename: str = Field(..., description="Filename with extension (pdf, tiff, jpg, png)")
    folder: Optional[str] = Field("", description="Folder/prefix in bronze")
    content_type: Optional[str] = Field(None, description="Content-Type, default application/pdf")

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UserCreate(BaseModel):
    email: EmailStr
    password: str
    role: str = Field(..., description="admin|verifier|viewer")


class JobCreate(BaseModel):
    customer: str
    name: str
    job_type: str
    priority: int = 0
    folder_prefix: str
    active_verifiers: int = 0


class AccessCreate(BaseModel):
    user_id: int
    folder_prefix: str

class AccessBulk(BaseModel):
    user_id: int
    folders: List[str]

class JobConfigBody(BaseModel):
    job: str
    config: dict


class PresencePing(BaseModel):
    folder_prefix: str


class AutomationToggleBody(BaseModel):
    enabled: bool


class ExportGenerateBody(BaseModel):
    key: str
    upload_ftp: bool = False

class BatchExportS3Body(BaseModel):
    job: str
    status: Optional[str] = None
    upload_ftp: bool = False
    mark_exported: bool = True


class NormalizedUpdate(BaseModel):
    field_name: str
    value: str


class DocumentUpdate(BaseModel):
    key: str = Field(..., description="S3 key under textract-results/")
    status: Optional[str] = Field(None, description="Approved/Rejected/Hold/To Review/Exported")
    fields: Optional[List[NormalizedUpdate]] = Field(None, description="Fields from normalized_data to update")
    reason: Optional[str] = Field(None, description="Optional rejection reason")


class DocumentValidateBody(BaseModel):
    key: str = Field(..., description="S3 key under textract-results/")
    fields: Optional[List[NormalizedUpdate]] = Field(None, description="Optional field overrides to validate (not persisted)")

class DocumentLookupBody(BaseModel):
    key: str = Field(..., description="S3 key under textract-results/")
    fields: Optional[List[NormalizedUpdate]] = Field(None, description="Optional field overrides to lookup against (not persisted)")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False, default="viewer")
    created_at = Column(DateTime, server_default=func.now())


class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    customer = Column(String, nullable=False)
    name = Column(String, nullable=False)
    job_type = Column(String, nullable=False)
    priority = Column(Integer, default=0)
    folder_prefix = Column(String, nullable=False)
    active_verifiers = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())


class UserAccess(Base):
    __tablename__ = "user_access"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    folder_prefix = Column(String, nullable=False)
    user = relationship("User")


class UserPresence(Base):
    __tablename__ = "user_presence"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    folder_prefix = Column(String, nullable=False, index=True)
    last_seen = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    user = relationship("User")


def seed_admin(db: Session):
    admin_email = os.getenv("ADMIN_EMAIL", "admin@example.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "ChangeMe123!")
    reset_flag = os.getenv("ADMIN_RESET", "0") in ("1", "true", "True")

    admin_email = normalize_email(admin_email)
    user = db.query(User).filter(func.lower(User.email) == admin_email).first()
    if user:
        if reset_flag:
            user.password_hash = hash_password(admin_password)
            db.add(user)
            db.commit()
        return
    admin = User(email=admin_email, password_hash=hash_password(admin_password), role="admin")
    db.add(admin)
    db.commit()


def init_db():
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        seed_admin(db)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)

def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def create_token(user: User) -> str:
    payload = {
        "sub": str(user.id),
        "email": user.email,
        "role": user.role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def get_current_user(db: Session = Depends(get_db), authorization: Optional[str] = Header(None)) -> User:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = int(payload["sub"])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def require_roles(*roles):
    def checker(user: User = Depends(get_current_user)):
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Forbidden")
        return user
    return checker


def get_allowed_folders(user: User, db: Session) -> List[str]:
    if user.role == "admin":
        return ["*"]
    access = db.query(UserAccess).filter(UserAccess.user_id == user.id).all()
    return [a.folder_prefix for a in access]


def ensure_folder_allowed(user: User, folder: str, db: Session):
    allowed = get_allowed_folders(user, db)
    if "*" in allowed:
        return
    if not folder:
        # empty prefix not allowed unless wildcard
        raise HTTPException(status_code=403, detail="Folder not allowed")
    cleaned = folder.strip("/")
    if cleaned not in allowed:
        raise HTTPException(status_code=403, detail="Folder not allowed")


def fetch_documents_for_export(job: str, status: Optional[str], limit: int = 1000) -> List[dict]:
    job = sanitize_folder(job)
    items: List[dict] = []
    s3_prefix = f"textract-results/{job.strip('/')}/"
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=s3_prefix, PaginationConfig={"MaxItems": limit}):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".json"):
                continue
            try:
                data = S3.get_object(Bucket=SILVER_BUCKET, Key=key)
                payload = json.loads(data["Body"].read())
                if status and canonical_status(payload.get("status")) != status:
                    continue
                if payload.get("page_count") is None:
                    payload["page_count"] = payload.get("pages") or ""
                payload["key"] = key
                items.append(payload)
                if len(items) >= limit:
                    return items
            except Exception:
                continue
    return items


app = FastAPI(title="PIQNIC Orbit")
origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
def startup_event():
    init_db()
    try:
        load_reference_tables()
    except Exception:
        pass


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/healthz")
def healthz():
    return JSONResponse({"status": "ok"})


@app.post("/auth/login")
def login(body: LoginRequest, db: Session = Depends(get_db)):
    email = normalize_email(body.email)
    user = db.query(User).filter(func.lower(User.email) == email).first()
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"token": create_token(user), "role": user.role, "email": user.email}


@app.get("/api/me")
def me(user: User = Depends(get_current_user)):
    return {"email": user.email, "role": user.role, "id": user.id}


@app.post("/api/presence")
def presence_ping(
    body: PresencePing,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    folder = sanitize_folder(body.folder_prefix)
    ensure_folder_allowed(user, folder, db)
    now = datetime.utcnow()
    row = (
        db.query(UserPresence)
        .filter(UserPresence.user_id == user.id)
        .filter(UserPresence.folder_prefix == folder)
        .first()
    )
    if row:
        row.last_seen = now
    else:
        row = UserPresence(user_id=user.id, folder_prefix=folder, last_seen=now)
    db.add(row)
    db.commit()
    return {"status": "ok", "folder_prefix": folder, "last_seen": now.isoformat()}


@app.get("/api/automation")
def api_get_automation(_: User = Depends(require_roles("admin"))):
    return get_automation_config()


@app.post("/admin/automation")
def admin_set_automation(body: AutomationToggleBody, user: User = Depends(require_roles("admin"))):
    save_automation_config(body.enabled, user.email)
    return {"status": "saved", **get_automation_config()}


@app.post("/admin/users")
def create_user(body: UserCreate, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    email = normalize_email(body.email)
    if db.query(User).filter(func.lower(User.email) == email).first():
        raise HTTPException(status_code=400, detail="User already exists")
    user = User(email=email, password_hash=hash_password(body.password), role=body.role)
    db.add(user)
    db.commit()
    return {"id": user.id, "email": user.email, "role": user.role}


@app.delete("/admin/users/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db), admin: User = Depends(require_roles("admin"))):
    if user_id == admin.id:
        raise HTTPException(status_code=400, detail="Cannot delete self")
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    # Clean up access records explicitly for sqlite compatibility
    db.query(UserAccess).filter(UserAccess.user_id == user_id).delete()
    db.delete(user)
    db.commit()
    return {"status": "deleted", "id": user_id}


@app.get("/admin/users")
def list_users(db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    users = db.query(User).all()
    return [{"id": u.id, "email": u.email, "role": u.role, "created_at": u.created_at} for u in users]


@app.get("/admin/access")
def list_access(db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    rows = db.query(UserAccess).all()
    return [{"id": r.id, "user_id": r.user_id, "folder_prefix": r.folder_prefix} for r in rows]


@app.post("/admin/access")
def create_access(body: AccessCreate, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    if not db.get(User, body.user_id):
        raise HTTPException(status_code=404, detail="User not found")
    row = UserAccess(user_id=body.user_id, folder_prefix=body.folder_prefix.strip("/"))
    db.add(row)
    db.commit()
    return {"id": row.id, "user_id": row.user_id, "folder_prefix": row.folder_prefix}


@app.post("/admin/access/bulk")
def set_access_bulk(body: AccessBulk, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    user = db.get(User, body.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    folders = []
    for f in body.folders or []:
        cleaned = f.strip("/")
        if cleaned and cleaned not in folders:
            folders.append(cleaned)
    db.query(UserAccess).filter(UserAccess.user_id == body.user_id).delete()
    for f in folders:
        db.add(UserAccess(user_id=body.user_id, folder_prefix=f))
    db.commit()
    return {"user_id": body.user_id, "folders": folders}


@app.delete("/admin/access/{access_id}")
def delete_access(access_id: int, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    row = db.get(UserAccess, access_id)
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    db.delete(row)
    db.commit()
    return {"status": "deleted"}


@app.get("/admin/access/summary")
def access_summary(db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    users = db.query(User).all()
    access_rows = db.query(UserAccess).all()
    access_map = {}
    for r in access_rows:
        access_map.setdefault(r.user_id, []).append(r.folder_prefix)
    jobs = db.query(Job).all()
    job_prefixes = [j.folder_prefix for j in jobs]
    return [
        {
            "user_id": u.id,
            "email": u.email,
            "role": u.role,
            "folders": sorted(access_map.get(u.id, [])),
            "jobs": job_prefixes if u.role == "admin" else [p for p in access_map.get(u.id, []) if p in job_prefixes],
        }
        for u in users
    ]


@app.post("/admin/jobs")
def create_job(body: JobCreate, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    job = Job(
        customer=body.customer,
        name=body.name,
        job_type=body.job_type,
        priority=body.priority,
        folder_prefix=body.folder_prefix,
        active_verifiers=body.active_verifiers,
    )
    db.add(job)
    db.commit()
    return {"id": job.id}


@app.get("/jobs")
def list_jobs(db: Session = Depends(get_db), user: User = Depends(require_roles("admin", "verifier", "viewer"))):
    allowed = get_allowed_folders(user, db)
    q = db.query(Job).order_by(Job.priority.desc(), Job.id.desc())
    if "*" not in allowed and user.role != "admin":
        q = q.filter(Job.folder_prefix.in_(allowed))
    jobs = q.all()

    prefixes = [j.folder_prefix for j in jobs]
    pending_counts = list_pending_counts(prefixes) if prefixes else {}

    # build map: folder_prefix -> list of assigned verifier emails (role=verifier + access)
    verifier_map: dict[str, list[str]] = {}
    if prefixes:
        access_rows = (
            db.query(UserAccess, User)
            .join(User, UserAccess.user_id == User.id)
            .filter(UserAccess.folder_prefix.in_(prefixes))
            .filter(User.role == "verifier")
            .all()
        )
        for access, usr in access_rows:
            verifier_map.setdefault(access.folder_prefix, []).append(usr.email)

    # active verifiers are those who pinged presence recently
    active_map: dict[str, list[str]] = {}
    cutoff = datetime.utcnow() - timedelta(minutes=int(os.getenv("PRESENCE_ACTIVE_MINUTES", "5")))
    if prefixes:
        pres_rows = (
            db.query(UserPresence, User)
            .join(User, UserPresence.user_id == User.id)
            .filter(UserPresence.folder_prefix.in_(prefixes))
            .filter(UserPresence.last_seen >= cutoff)
            .filter(User.role == "verifier")
            .all()
        )
        for pres, usr in pres_rows:
            active_map.setdefault(pres.folder_prefix, []).append(usr.email)

    visible = []
    for j in jobs:
        # hide jobs with no data in S3/bronze
        if not prefix_has_data(j.folder_prefix):
            continue
        visible.append(j)

    return [
        {
            "id": j.id,
            "customer": j.customer,
            "name": j.name,
            "job_type": j.job_type,
            "priority": j.priority,
            "folder_prefix": j.folder_prefix,
            "active_verifiers": sorted(set(active_map.get(j.folder_prefix, []))),
            "assigned_verifiers": sorted(set(verifier_map.get(j.folder_prefix, []))),
            "documents_to_review": pending_counts.get(j.folder_prefix, 0),
        }
        for j in visible
    ]


@app.post("/api/upload-url")
def upload_url(body: UploadUrlRequest, user: User = Depends(require_roles("admin", "verifier")), db: Session = Depends(get_db)):
    folder = sanitize_folder(body.folder)
    ensure_folder_allowed(user, folder, db)
    filename = body.filename
    if not filename:
        raise HTTPException(status_code=400, detail="filename is required")
    content_type = body.content_type or "application/pdf"
    key = build_key(folder, filename)
    url = presign_put(BRONZE_BUCKET, key, content_type)
    return {"bucket": BRONZE_BUCKET, "key": key, "uploadUrl": url, "contentType": content_type, "method": "PUT"}


@app.get("/api/textract")
def textract_status(
    folder: Optional[str] = None,
    filename: Optional[str] = None,
    key: Optional[str] = None,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    if not filename:
        if not key:
            raise HTTPException(status_code=400, detail="filename or key is required")
        filename = os.path.basename(key)
        folder = sanitize_folder(folder or os.path.dirname(key))
    else:
        folder = sanitize_folder(folder)

    ensure_folder_allowed(user, folder, db)

    keys = candidate_textract_keys(folder, filename)

    for candidate in keys:
        if object_exists(SILVER_BUCKET, candidate):
            obj = S3.get_object(Bucket=SILVER_BUCKET, Key=candidate)
            data = json.loads(obj["Body"].read())
            return {"status": "ready", "key": candidate, "url": presign_get(SILVER_BUCKET, candidate), "data": data}

    return {"status": "pending", "tried": keys}


@app.get("/api/export")
def export_link(
    folder: Optional[str] = None,
    filename: Optional[str] = None,
    key: Optional[str] = None,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    if not filename:
        if not key:
            raise HTTPException(status_code=400, detail="filename or key is required")
        filename = os.path.basename(key)
        folder = sanitize_folder(folder or os.path.dirname(key))
    else:
        folder = sanitize_folder(folder)

    ensure_folder_allowed(user, folder, db)

    keys = candidate_export_keys(folder, filename)
    for candidate in keys:
        if object_exists(SILVER_BUCKET, candidate):
            return {"status": "ready", "key": candidate, "url": presign_get(SILVER_BUCKET, candidate)}
    return {"status": "pending", "tried": keys}


@app.get("/api/export/batch")
def export_batch(
    job: str,
    status: Optional[str] = None,
    user: User = Depends(require_roles("admin", "verifier")),
    db: Session = Depends(get_db),
):
    job = sanitize_folder(job)
    ensure_folder_allowed(user, job, db)
    rows = fetch_documents_for_export(job, status=status, limit=2000)
    return stream_csv_zip(job, rows)


@app.post("/api/export/generate")
def export_generate(
    body: ExportGenerateBody,
    user: User = Depends(require_roles("admin", "verifier")),
    db: Session = Depends(get_db),
):
    key = body.key
    if not key.startswith("textract-results/"):
        raise HTTPException(status_code=400, detail="Key must be under textract-results/")
    parts = key.split("/", 2)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid key")
    job_prefix = parts[1]
    ensure_folder_allowed(user, job_prefix, db)

    payload = load_json_from_s3(key)
    if not source_exists(payload):
        raise HTTPException(status_code=404, detail="Source file missing in bucket")

    status = canonical_status(payload.get("status"))
    if status in ("To Review", "Hold"):
        raise HTTPException(status_code=400, detail=f"Cannot export document in status '{status}'")

    source_bucket = payload.get("source_bucket") or BRONZE_BUCKET
    source_key = payload.get("source_file") or payload.get("source_key")
    if not source_key:
        raise HTTPException(status_code=400, detail="source_key missing in payload")

    src_obj = S3.get_object(Bucket=source_bucket, Key=source_key)
    source_bytes = src_obj["Body"].read()
    source_filename = os.path.basename(source_key)

    export_key = candidate_export_keys(job_prefix, source_filename)[0]
    zip_bytes = build_export_zip_bytes(job_prefix, payload, source_filename, source_bytes)

    S3.put_object(
        Bucket=SILVER_BUCKET,
        Key=export_key,
        Body=zip_bytes,
        ContentType="application/zip",
    )

    payload["status"] = "Exported"
    payload["export_key"] = export_key
    payload["exported_at"] = datetime.utcnow().isoformat()
    payload["exported_by"] = user.email
    payload["updated_at"] = payload.get("exported_at")
    payload["updated_by"] = user.email
    save_json_to_s3(key, payload)

    cfg = get_job_config(job_prefix) or {}
    auto_ftp = bool(cfg.get("auto_upload_ftp"))
    ftp_uploaded = False
    ftp_error = None
    if (body.upload_ftp or auto_ftp) and EXPORT_FTP_HOST:
        try:
            remote_name = f"{job_prefix}/{os.path.basename(export_key)}"
            _ftp_upload_bytes(remote_name, zip_bytes)
            ftp_uploaded = True
        except Exception as exc:
            ftp_error = str(exc)

    return {
        "status": "ok",
        "export_key": export_key,
        "url": presign_get(SILVER_BUCKET, export_key),
        "ftp_uploaded": ftp_uploaded,
        "ftp_error": ftp_error,
    }


@app.post("/api/export/batch/s3")
def export_batch_to_s3(
    body: BatchExportS3Body,
    user: User = Depends(require_roles("admin")),
    db: Session = Depends(get_db),
):
    job = sanitize_folder(body.job)
    ensure_folder_allowed(user, job, db)

    status = canonical_status(body.status) if body.status else None
    rows = fetch_documents_for_export(job, status=status, limit=5000)

    export_csv = build_export_csv_bytes(job, rows)
    meta_csv = build_meta_csv_bytes(rows)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    status_tag = (status or "all").replace(" ", "_")
    out_key = f"portal/exports/batch/{job}/{ts}_{status_tag}.zip"

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("export.csv", export_csv)
        zf.writestr("meta.csv", meta_csv)
    output.seek(0)
    zip_bytes = output.getvalue()

    S3.put_object(
        Bucket=SILVER_BUCKET,
        Key=out_key,
        Body=zip_bytes,
        ContentType="application/zip",
    )

    # Optionally mark exported
    if body.mark_exported and status in ("Approved", "Rejected"):
        for p in rows:
            doc_key = p.get("key")
            if not doc_key:
                continue
            try:
                p["status"] = "Exported"
                p["exported_at"] = datetime.utcnow().isoformat()
                p["exported_by"] = user.email
                p["export_key"] = p.get("export_key") or ""
                save_json_to_s3(doc_key, p)
            except Exception:
                continue

    cfg = get_job_config(job) or {}
    auto_ftp = bool(cfg.get("auto_upload_ftp"))
    ftp_uploaded = False
    ftp_error = None
    if (body.upload_ftp or auto_ftp) and EXPORT_FTP_HOST:
        try:
            remote_name = f"{job}/batch/{os.path.basename(out_key)}"
            _ftp_upload_bytes(remote_name, zip_bytes)
            ftp_uploaded = True
        except Exception as exc:
            ftp_error = str(exc)

    return {
        "status": "ok",
        "s3_key": out_key,
        "url": presign_get(SILVER_BUCKET, out_key),
        "count": len(rows),
        "ftp_uploaded": ftp_uploaded,
        "ftp_error": ftp_error,
    }

@app.get("/api/customers/lookup")
def customer_lookup(
    customer_id: str,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
):
    cid = (customer_id or "").strip()
    if not cid:
        raise HTTPException(status_code=400, detail="customer_id required")
    key = cid.upper()
    matches = REFERENCE_DATA["indexes"].get("by_id", {}).get(key, [])
    if not matches:
        return {"found": False, "status": "no_match", "match_count": 0, "customer_id": cid, "data": []}
    # Build simplified response
    resp = []
    for m in matches:
        resp.append(
            {
                "customer_id": m.get("customer_id") or m.get("Cre Account") or "",
                "name": m.get("name") or m.get("Cre Name") or "",
                "gst_abn": m.get("gst_abn") or "",
                "currency": m.get("currency") or "",
                "source": m.get("source") or "reference",
                "raw": m,
            }
        )
    status = "match" if len(resp) == 1 else "ambiguous"
    return {
        "found": True,
        "status": status,
        "match_count": len(resp),
        "customer_id": cid,
        "data": resp,
        "selected": resp[0] if status == "match" else None,
    }


@app.post("/admin/reference/reload")
def admin_reference_reload(_: User = Depends(require_roles("admin"))):
    load_reference_tables()
    return {"status": "reloaded", "suppliers": len(REFERENCE_DATA["suppliers"])}


@app.post("/admin/reference/upload")
def admin_reference_upload(file: UploadFile = File(...), _: User = Depends(require_roles("admin"))):
    filename = file.filename or "upload.csv"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", filename)
    payload = file.file.read()

    # Persist to S3 so updates survive redeploys.
    if REFERENCE_BUCKET:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        active_key = f"{REFERENCE_PREFIX}/{safe_name}" if REFERENCE_PREFIX else safe_name
        version_key = f"{REFERENCE_PREFIX}/versions/{ts}/{safe_name}" if REFERENCE_PREFIX else f"versions/{ts}/{safe_name}"
        S3.put_object(Bucket=REFERENCE_BUCKET, Key=version_key, Body=payload)
        S3.put_object(Bucket=REFERENCE_BUCKET, Key=active_key, Body=payload)

    # Also keep a local copy (useful for local dev); best-effort.
    try:
        dest = REFERENCE_DIR / safe_name
        REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as f:
            f.write(payload)
    except Exception:
        pass

    # reload after upload
    load_reference_tables()
    return {
        "status": "uploaded",
        "file": safe_name,
        "s3_key": (f"{REFERENCE_PREFIX}/{safe_name}" if REFERENCE_PREFIX else safe_name) if REFERENCE_BUCKET else None,
        "version_key": (f"{REFERENCE_PREFIX}/versions/{ts}/{safe_name}" if REFERENCE_PREFIX else f"versions/{ts}/{safe_name}") if REFERENCE_BUCKET else None,
        "suppliers": len(REFERENCE_DATA["suppliers"]),
    }


@app.post("/admin/reference/import-ftp")
def admin_reference_import_ftp(user: User = Depends(require_roles("admin"))):
    if not (REFERENCE_FTP_HOST and REFERENCE_FTP_USER and REFERENCE_FTP_PASSWORD):
        raise HTTPException(status_code=400, detail="Reference FTP env is not configured")
    imported = []
    errors = []
    files = REFERENCE_FTP_FILES
    if not files:
        # default behavior: try to list remote dir and import all .csv
        try:
            ftp = FTP_TLS(REFERENCE_FTP_HOST) if REFERENCE_FTP_TLS else FTP(REFERENCE_FTP_HOST)
            ftp.login(REFERENCE_FTP_USER, REFERENCE_FTP_PASSWORD)
            if REFERENCE_FTP_TLS and isinstance(ftp, FTP_TLS):
                ftp.prot_p()
            if REFERENCE_FTP_DIR:
                ftp.cwd(REFERENCE_FTP_DIR)
            files = [f for f in (ftp.nlst() or []) if str(f).lower().endswith(".csv")]
            try:
                ftp.quit()
            except Exception:
                ftp.close()
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"FTP list failed: {exc}")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    for name in files:
        try:
            safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", os.path.basename(name))
            data = _ftp_download_bytes(name)
            if not data:
                raise RuntimeError("Empty file")
            if REFERENCE_BUCKET:
                active_key = f"{REFERENCE_PREFIX}/{safe_name}" if REFERENCE_PREFIX else safe_name
                version_key = f"{REFERENCE_PREFIX}/versions/{ts}/{safe_name}" if REFERENCE_PREFIX else f"versions/{ts}/{safe_name}"
                S3.put_object(Bucket=REFERENCE_BUCKET, Key=version_key, Body=data)
                S3.put_object(Bucket=REFERENCE_BUCKET, Key=active_key, Body=data)
                imported.append({"file": safe_name, "version_key": version_key, "active_key": active_key})
            else:
                imported.append({"file": safe_name, "version_key": None, "active_key": None})
            # also write local best-effort
            try:
                REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
                (REFERENCE_DIR / safe_name).write_bytes(data)
            except Exception:
                pass
        except Exception as exc:
            errors.append({"file": name, "error": str(exc)})

    # reload after import
    try:
        load_reference_tables()
    except Exception:
        pass
    return {"status": "ok", "imported": imported, "errors": errors, "suppliers": len(REFERENCE_DATA["suppliers"])}


@app.get("/api/job-config")
def api_job_config(
    job: str,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    job = sanitize_folder(job)
    ensure_folder_allowed(user, job, db)
    return {"job": job, "config": get_job_config(job)}


@app.post("/admin/job-config")
def admin_job_config_save(
    body: JobConfigBody,
    user: User = Depends(require_roles("admin")),
):
    job = sanitize_folder(body.job)
    save_job_config(job, body.config)
    return {"status": "saved", "job": job, "s3_key": job_config_s3_key(job)}


@app.get("/api/reports/monthly")
def report_monthly(
    month: Optional[str] = None,
    job: Optional[str] = None,
    user: User = Depends(require_roles("admin")),
    db: Session = Depends(get_db),
):
    """
    Monthly reporting per job: document count and total pages, with status breakdown.
    """
    if month is None:
        month = datetime.utcnow().strftime("%Y-%m")
    month = (month or "").strip()
    if not MONTH_RE.match(month):
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    year, mon = month.split("-", 1)
    start = datetime(int(year), int(mon), 1)
    if int(mon) == 12:
        end = datetime(int(year) + 1, 1, 1)
    else:
        end = datetime(int(year), int(mon) + 1, 1)

    q = db.query(Job).order_by(Job.priority.desc(), Job.id.desc())
    if job:
        job = sanitize_folder(job)
        q = q.filter(Job.folder_prefix == job)
    jobs = q.all()

    def blank_totals():
        return {"documents": 0, "pages": 0, "by_status": {s: 0 for s in ("To Review", "Approved", "Rejected", "Hold", "Exported")}}

    totals = blank_totals()
    out = []

    for j in jobs:
        job_prefix = j.folder_prefix
        if not job_prefix:
            continue
        pref = f"textract-results/{job_prefix.strip('/')}/"
        paginator = S3.get_paginator("list_objects_v2")
        stats = blank_totals()
        for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=pref):
            for obj in page.get("Contents", []):
                key = obj.get("Key") or ""
                if not key.lower().endswith(".json"):
                    continue
                try:
                    payload = load_json_from_s3(key)
                except Exception:
                    continue

                dt = parse_payload_datetime(payload.get("processed_at")) or parse_payload_datetime(payload.get("updated_at")) or obj.get("LastModified")
                dt = parse_payload_datetime(dt)
                if not dt:
                    continue
                if not (start <= dt < end):
                    continue

                stats["documents"] += 1
                st = canonical_status(payload.get("status"))
                stats["by_status"][st] = stats["by_status"].get(st, 0) + 1
                pages = payload.get("page_count") or payload.get("pages") or 0
                try:
                    pages = int(pages)
                except Exception:
                    pages = 0
                stats["pages"] += max(pages, 0)
        if stats["documents"] == 0:
            continue
        totals["documents"] += stats["documents"]
        totals["pages"] += stats["pages"]
        for k, v in stats["by_status"].items():
            totals["by_status"][k] = totals["by_status"].get(k, 0) + v
        out.append(
            {
                "job": job_prefix,
                "customer": j.customer,
                "name": j.name,
                "job_type": j.job_type,
                **stats,
            }
        )

    return {"month": month, "jobs": out, "totals": totals}


def _billing_blank_totals() -> dict:
    return {
        "documents": 0,
        "pages": 0,
        "by_status": {s: 0 for s in ("To Review", "Approved", "Rejected", "Hold", "Exported")},
        "operators": {},
        "openai_input_tokens": 0,
        "openai_output_tokens": 0,
        "openai_total_tokens": 0,
    }


def _billing_accumulate(stats: dict, payload: dict):
    stats["documents"] += 1
    st = canonical_status(payload.get("status"))
    stats["by_status"][st] = stats["by_status"].get(st, 0) + 1
    pages = payload.get("page_count") or payload.get("pages") or 0
    try:
        pages = int(pages)
    except Exception:
        pages = 0
    stats["pages"] += max(pages, 0)

    updated_by = (payload.get("updated_by") or "").strip()
    exported_by = (payload.get("exported_by") or "").strip()
    if updated_by:
        stats["operators"].setdefault(updated_by, {"updated": 0, "exported": 0})
        stats["operators"][updated_by]["updated"] += 1
    if exported_by:
        stats["operators"].setdefault(exported_by, {"updated": 0, "exported": 0})
        stats["operators"][exported_by]["exported"] += 1

    usage = ((payload.get("openai") or {}).get("usage") or {})
    if isinstance(usage, dict):
        for src, dst in (
            ("input_tokens", "openai_input_tokens"),
            ("output_tokens", "openai_output_tokens"),
            ("total_tokens", "openai_total_tokens"),
        ):
            try:
                stats[dst] = int(stats.get(dst) or 0) + int(usage.get(src) or 0)
            except Exception:
                continue


def compute_billing_monthly(db: Session, month: str, job: Optional[str] = None) -> dict:
    month = (month or "").strip()
    if not MONTH_RE.match(month):
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    year, mon = month.split("-", 1)
    start = datetime(int(year), int(mon), 1)
    if int(mon) == 12:
        end = datetime(int(year) + 1, 1, 1)
    else:
        end = datetime(int(year), int(mon) + 1, 1)

    q = db.query(Job).order_by(Job.priority.desc(), Job.id.desc())
    if job:
        job = sanitize_folder(job)
        q = q.filter(Job.folder_prefix == job)
    jobs = q.all()

    groups: Dict[Tuple[str, str], dict] = {}
    totals = _billing_blank_totals()

    for j in jobs:
        job_prefix = j.folder_prefix
        if not job_prefix:
            continue
        pref = f"textract-results/{job_prefix.strip('/')}/"
        paginator = S3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=SILVER_BUCKET, Prefix=pref):
            for obj in page.get("Contents", []):
                key = obj.get("Key") or ""
                if not key.lower().endswith(".json"):
                    continue
                try:
                    payload = load_json_from_s3(key)
                except Exception:
                    continue

                dt = parse_payload_datetime(payload.get("processed_at")) or parse_payload_datetime(payload.get("updated_at")) or obj.get("LastModified")
                dt = parse_payload_datetime(dt)
                if not dt or not (start <= dt < end):
                    continue

                gk = (j.customer or "", j.job_type or "")
                group = groups.get(gk)
                if not group:
                    group = {"customer": j.customer, "job_type": j.job_type, "jobs": [], **_billing_blank_totals()}
                    groups[gk] = group
                if job_prefix not in group["jobs"]:
                    group["jobs"].append(job_prefix)

                _billing_accumulate(group, payload)
                _billing_accumulate(totals, payload)

    out = list(groups.values())
    out.sort(key=lambda r: (str(r.get("customer") or ""), str(r.get("job_type") or "")))
    return {"month": month, "groups": out, "totals": totals}


@app.get("/api/billing/monthly")
def billing_monthly(
    month: Optional[str] = None,
    job: Optional[str] = None,
    user: User = Depends(require_roles("admin")),
    db: Session = Depends(get_db),
):
    if month is None:
        month = datetime.utcnow().strftime("%Y-%m")
    return compute_billing_monthly(db=db, month=month, job=job)


@app.get("/api/billing/export")
def billing_export_csv(
    month: Optional[str] = None,
    job: Optional[str] = None,
    user: User = Depends(require_roles("admin")),
    db: Session = Depends(get_db),
):
    if month is None:
        month = datetime.utcnow().strftime("%Y-%m")
    data = compute_billing_monthly(db=db, month=month, job=job)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "customer",
            "job_type",
            "jobs",
            "documents",
            "pages",
            "to_review",
            "approved",
            "rejected",
            "hold",
            "exported",
            "openai_input_tokens",
            "openai_output_tokens",
            "openai_total_tokens",
        ]
    )
    for g in data.get("groups") or []:
        by = g.get("by_status") or {}
        writer.writerow(
            [
                g.get("customer") or "",
                g.get("job_type") or "",
                ";".join(g.get("jobs") or []),
                g.get("documents") or 0,
                g.get("pages") or 0,
                by.get("To Review", 0),
                by.get("Approved", 0),
                by.get("Rejected", 0),
                by.get("Hold", 0),
                by.get("Exported", 0),
                g.get("openai_input_tokens") or 0,
                g.get("openai_output_tokens") or 0,
                g.get("openai_total_tokens") or 0,
            ]
        )
    csv_bytes = output.getvalue().encode("utf-8")
    filename = f"billing_{(data.get('month') or 'month')}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/documents")
def list_documents(
    job: str,
    status: Optional[str] = None,
    cursor: Optional[str] = None,
    page_size: int = 50,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    job = sanitize_folder(job)
    ensure_folder_allowed(user, job, db)

    items = []
    status_filter = status.lower() if status else None

    prefix = f"textract-results/{job}/"
    paginator = S3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(
        Bucket=SILVER_BUCKET,
        Prefix=prefix,
        PaginationConfig={
            "MaxItems": page_size * 5,  # fetch a window to sort by LastModified
            "PageSize": 200,
            **({"StartingToken": cursor} if cursor else {}),
        },
    )

    collected = []
    next_token = None
    for page in page_iter:
        next_token = page.get("NextToken") or page.get("NextContinuationToken")
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".json"):
                continue
            collected.append(obj)

    collected.sort(key=lambda o: o.get("LastModified"), reverse=True)

    for obj in collected:
        key = obj["Key"]
        try:
            payload = load_json_from_s3(key)
        except Exception:
            continue
        if not source_exists(payload):
            # auto-remove orphan JSON so it does not appear in UI
            try:
                S3.delete_object(Bucket=SILVER_BUCKET, Key=key)
            except Exception:
                pass
            continue
        doc_status = canonical_status(payload.get("status"))
        if status_filter and doc_status.lower() != status_filter:
            continue
        normalized = payload.get("normalized_data") or []
        normalized = apply_reference_validation(normalized)
        fields_map = {str(f.get("key") or f.get("field_name") or "").strip(): f for f in normalized}

        def val(name): return (fields_map.get(name) or {}).get("value")

        items.append({
            "key": key,
            "name": os.path.basename(key),
            "status": doc_status,
            "created_at": payload.get("processed_at") or obj.get("LastModified"),
            "invoice_number": val("Invoice Number"),
            "invoice_date": val("Invoice Date"),
            "total_amount": val("Total Amount") or val("Invoice Total"),
            "gst_amount": val("GST Amount"),
            "supplier": val("Supplier/Vendor Name") or val("Creditor Name"),
            "page_count": payload.get("page_count") or payload.get("pages"),
        })
        if len(items) >= page_size:
            break

    return {
        "items": items[:page_size],
        "next_cursor": next_token,
        "truncated": bool(next_token),
    }


@app.get("/api/documents/detail")
def document_detail(
    key: str,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    if not key.startswith("textract-results/"):
        raise HTTPException(status_code=400, detail="Key must be under textract-results/")
    parts = key.split("/", 2)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid key")
    job_prefix = parts[1]
    ensure_folder_allowed(user, job_prefix, db)

    try:
        payload = load_json_from_s3(key)
    except Exception:
        raise HTTPException(status_code=404, detail="Document JSON missing")
    if not source_exists(payload):
        # prune orphan JSON
        try:
            S3.delete_object(Bucket=SILVER_BUCKET, Key=key)
        except Exception:
            pass
        raise HTTPException(status_code=404, detail="Source file missing in bucket")
    source_bucket = payload.get("source_bucket") or BRONZE_BUCKET
    source_key = payload.get("source_file") or payload.get("source_key")
    pdf_url = None
    if source_key:
        try:
            pdf_url = presign_get(source_bucket, source_key)
        except Exception:
            pdf_url = None

    if payload.get("normalized_data"):
        payload["normalized_data"] = apply_reference_validation(payload["normalized_data"])

    return {"payload": payload, "pdf_url": pdf_url, "key": key, "ddb": None, "source_bucket": source_bucket, "source_key": source_key}


@app.post("/api/documents/validate")
def validate_document_api(
    body: DocumentValidateBody,
    user: User = Depends(require_roles("admin", "verifier")),
    db: Session = Depends(get_db),
):
    key = body.key
    if not key.startswith("textract-results/"):
        raise HTTPException(status_code=400, detail="Key must be under textract-results/")
    parts = key.split("/", 2)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid key")
    job_prefix = parts[1]
    ensure_folder_allowed(user, job_prefix, db)

    payload = load_json_from_s3(key)
    if body.fields:
        normalized = payload.get("normalized_data") or []
        for upd in body.fields:
            found = False
            for item in normalized:
                k = item.get("key") or item.get("field_name")
                if k and k.strip().lower() == upd.field_name.strip().lower():
                    item["value"] = upd.value
                    found = True
                    break
            if not found:
                normalized.append(
                    {
                        "key": upd.field_name,
                        "value": upd.value,
                        "confidence": 0,
                    }
                )
        payload["normalized_data"] = normalized

    errors = validate_document(payload, job_prefix)
    return {"job": job_prefix, "errors": errors}


@app.post("/api/documents/lookup")
def lookup_document_api(
    body: DocumentLookupBody,
    user: User = Depends(require_roles("admin", "verifier", "viewer")),
    db: Session = Depends(get_db),
):
    key = body.key
    if not key.startswith("textract-results/"):
        raise HTTPException(status_code=400, detail="Key must be under textract-results/")
    parts = key.split("/", 2)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid key")
    job_prefix = parts[1]
    ensure_folder_allowed(user, job_prefix, db)

    payload = load_json_from_s3(key)
    if body.fields:
        normalized = payload.get("normalized_data") or []
        for upd in body.fields:
            found = False
            for item in normalized:
                k = item.get("key") or item.get("field_name")
                if k and k.strip().lower() == upd.field_name.strip().lower():
                    item["value"] = upd.value
                    found = True
                    break
            if not found:
                normalized.append({"key": upd.field_name, "value": upd.value, "confidence": 0})
        payload["normalized_data"] = normalized

    return run_unified_lookup(payload, job_prefix)


@app.patch("/api/documents")
def update_document(
    body: DocumentUpdate,
    user: User = Depends(require_roles("admin", "verifier")),
    db: Session = Depends(get_db),
):
    key = body.key
    if not key.startswith("textract-results/"):
        raise HTTPException(status_code=400, detail="Key must be under textract-results/")
    parts = key.split("/", 2)
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid key")
    job_prefix = parts[1]
    ensure_folder_allowed(user, job_prefix, db)

    payload = load_json_from_s3(key)
    prev_payload = copy.deepcopy(payload)
    job_prefix, doc_id = parse_job_doc_from_key(key)

    if body.status:
        payload["status"] = canonical_status(body.status)
        payload["updated_at"] = datetime.utcnow().isoformat()
        payload["updated_by"] = user.email
        if payload["status"] == "Rejected" and body.reason:
            payload["reason"] = body.reason
        elif payload["status"] != "Rejected":
            payload.pop("reason", None)

    if body.fields:
        normalized = payload.get("normalized_data") or []
        for upd in body.fields:
            found = False
            for item in normalized:
                k = item.get("key") or item.get("field_name")
                if k and k.strip().lower() == upd.field_name.strip().lower():
                    item["value"] = upd.value
                    found = True
                    break
            if not found:
                normalized.append({
                    "key": upd.field_name,
                    "value": upd.value,
                    "confidence": 0,
                })
        payload["normalized_data"] = normalized

    # Validate before persisting when the document is in an exportable/approved state.
    current_status = canonical_status(payload.get("status"))
    payload["status"] = current_status
    if current_status in ("Approved", "Exported"):
        errors = validate_document(payload, job_prefix)
        if errors:
            raise HTTPException(status_code=400, detail={"message": "Validation failed", "errors": errors})

    save_json_to_s3(key, payload)

    # Audit log
    try:
        changes = []
        if body.status and (body.status != prev_payload.get("status")):
            changes.append({"field": "status", "old": prev_payload.get("status"), "new": body.status})
        if body.reason and (body.reason != prev_payload.get("reason")):
            changes.append({"field": "reason", "old": prev_payload.get("reason"), "new": body.reason})
        if body.fields:
            prev_norm = { (f.get("key") or f.get("field_name") or "").strip().lower(): f.get("value") for f in prev_payload.get("normalized_data") or [] }
            for upd in body.fields:
                key_lower = upd.field_name.strip().lower()
                old_val = prev_norm.get(key_lower)
                if old_val != upd.value:
                    changes.append({"field": upd.field_name, "old": old_val, "new": upd.value})
        write_audit({
            "action": "update_document",
            "job_prefix": job_prefix,
            "doc_id": doc_id,
            "user": user.email,
            "changes": changes,
            "status": payload.get("status"),
            "reason": payload.get("reason"),
            "updated_at": payload.get("updated_at"),
        })
    except Exception:
        pass

    return {"status": "ok", "key": key}


@app.get("/api/pdf")
def pdf_proxy(
    bucket: str,
    key: str,
    token: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """
    Serve PDF from S3. If token or Authorization is provided, try to auth; otherwise allow (to avoid viewer breakage).
    """
    # Make this endpoint permissive to avoid viewer breakage; auth is best-effort only.
    if not authorization and token:
        authorization = f"Bearer {token}"
    if authorization:
        try:
            user = get_current_user(db=db, authorization=authorization)
            folder = sanitize_folder(key.split("/", 1)[0] if "/" in key else "")
            ensure_folder_allowed(user, folder, db)
        except Exception:
            # Ignore auth errors so PDF rendering keeps working.
            pass

    try:
        obj = S3.get_object(Bucket=bucket, Key=key)
        stream = obj["Body"]
        return StreamingResponse(stream, media_type=obj.get("ContentType", "application/pdf"))
    except ClientError as e:
        raise HTTPException(status_code=404, detail=f"PDF not found: {e}")


if __name__ == "__main__":
    import uvicorn

    init_db()
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
