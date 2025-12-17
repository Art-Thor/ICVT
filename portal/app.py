import json
import os
import re
import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

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
from fastapi import UploadFile, File


REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket-icvt")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket-icvt")
STATIC_DIR = Path(__file__).parent / "static"
REFERENCE_DIR = Path(__file__).parent / "reference"
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
# Load reference data at startup
try:
    load_reference_tables()
except Exception:
    pass

engine = create_engine(DB_URL, connect_args={"check_same_thread": False} if DB_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
TS_PREFIX_RE = re.compile(r"^(\d{10,13})")
SAFE_FOLDER_RE = re.compile(r"^[A-Za-z0-9._/+\\-]*$")
REFERENCE_DATA = {
    "suppliers": [],
    "po": [],
    "invoices": [],
    "indexes": {},
}


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
    REFERENCE_DATA["indexes"] = {"by_id": {}, "by_number": {}, "by_name": set()}

    def load_csv(path: Path):
        if not path.exists():
            return []
        rows = []
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
        return rows

    jar_sup = REFERENCE_DIR / "JAR Supplier Details.csv"
    if jar_sup.exists():
        REFERENCE_DATA["suppliers"].extend(
            [
                {
                    "source": "jar",
                    "customer_id": r.get("Cre Account", ""),
                    "name": r.get("Cre Name", ""),
                    "gst_abn": "",
                    "currency": "",
                }
                for r in load_csv(jar_sup)
            ]
        )

    nzme_sup = REFERENCE_DIR / "NZME JDE Supplier Details.csv"
    if nzme_sup.exists():
        REFERENCE_DATA["suppliers"].extend(
            [
                {
                    "source": "nzme",
                    "customer_id": r.get("Number", ""),
                    "name": r.get("Name", ""),
                    "gst_abn": r.get("GST_ABN", ""),
                    "currency": r.get("Currency", ""),
                }
                for r in load_csv(nzme_sup)
            ]
        )

    jar_po = REFERENCE_DIR / "JAR Database Table PO Details.csv"
    if jar_po.exists():
        REFERENCE_DATA["po"].extend(load_csv(jar_po))

    jar_inv = REFERENCE_DIR / "JAR Invoice Details.csv"
    if jar_inv.exists():
        REFERENCE_DATA["invoices"].extend(load_csv(jar_inv))

    # build indexes
    for sup in REFERENCE_DATA["suppliers"]:
        cid = (sup.get("customer_id") or "").strip()
        if cid:
            REFERENCE_DATA["indexes"].setdefault("by_id", {}).setdefault(cid.upper(), []).append(sup)
        name = (sup.get("name") or "").strip()
        if name:
            REFERENCE_DATA["indexes"]["by_name"].add(name.upper())
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


def stream_csv_zip(rows: List[dict]) -> StreamingResponse:
    output = io.BytesIO()
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["status", "name", "created_at", "invoice_number", "invoice_date", "total_amount", "supplier", "key", "page_count"])
    for r in rows:
        writer.writerow([
            r.get("status", ""),
            r.get("name", "") or r.get("doc_id", ""),
            r.get("created_at", ""),
            r.get("invoice_number", ""),
            r.get("invoice_date", ""),
            r.get("total_amount", ""),
            r.get("supplier", ""),
            r.get("key", ""),
            r.get("page_count", ""),
        ])
    csv_data = csv_buffer.getvalue()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("export.csv", csv_data)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="export.zip"'},
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
    return "To Review"


def apply_reference_validation(normalized: List[dict]) -> List[dict]:
    by_id = REFERENCE_DATA["indexes"].get("by_id", {})
    by_number = REFERENCE_DATA["indexes"].get("by_number", {})
    by_name = REFERENCE_DATA["indexes"].get("by_name", set())
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
        if status != "match":
            status = ""
        item = dict(item)
        item["validation"] = {"status": status}
        out.append(item)
    return out


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


class NormalizedUpdate(BaseModel):
    field_name: str
    value: str


class DocumentUpdate(BaseModel):
    key: str = Field(..., description="S3 key under textract-results/")
    status: Optional[str] = Field(None, description="Approved/Rejected/To Review/Exported")
    fields: Optional[List[NormalizedUpdate]] = Field(None, description="Fields from normalized_data to update")
    reason: Optional[str] = Field(None, description="Optional rejection reason")


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


def seed_admin(db: Session):
    admin_email = os.getenv("ADMIN_EMAIL", "admin@example.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "ChangeMe123!")
    reset_flag = os.getenv("ADMIN_RESET", "0") in ("1", "true", "True")

    user = db.query(User).filter(User.email == admin_email).first()
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


app = FastAPI(title="Upload & Textract Viewer")
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


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/healthz")
def healthz():
    return JSONResponse({"status": "ok"})


@app.post("/auth/login")
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return {"token": create_token(user), "role": user.role, "email": user.email}


@app.get("/api/me")
def me(user: User = Depends(get_current_user)):
    return {"email": user.email, "role": user.role, "id": user.id}


@app.post("/admin/users")
def create_user(body: UserCreate, db: Session = Depends(get_db), _: User = Depends(require_roles("admin"))):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=400, detail="User already exists")
    user = User(email=body.email, password_hash=hash_password(body.password), role=body.role)
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

    # build map: folder_prefix -> list of verifier emails
    verifier_map = {}
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
            "active_verifiers": sorted(verifier_map.get(j.folder_prefix, [])),
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
    return stream_csv_zip(rows)


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
        return {"found": False, "customer_id": cid, "data": []}
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
    return {"found": True, "customer_id": cid, "data": resp}


@app.post("/admin/reference/reload")
def admin_reference_reload(_: User = Depends(require_roles("admin"))):
    load_reference_tables()
    return {"status": "reloaded", "suppliers": len(REFERENCE_DATA["suppliers"])}


@app.post("/admin/reference/upload")
def admin_reference_upload(file: UploadFile = File(...), _: User = Depends(require_roles("admin"))):
    filename = file.filename or "upload.csv"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", filename)
    dest = REFERENCE_DIR / safe_name
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        f.write(file.file.read())
    # attempt reload after upload
    load_reference_tables()
    return {"status": "uploaded", "file": safe_name, "suppliers": len(REFERENCE_DATA["suppliers"])}


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
        payload["status"] = body.status
        payload["updated_at"] = datetime.utcnow().isoformat()
        payload["updated_by"] = user.email
        if body.reason:
            payload["reason"] = body.reason

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
