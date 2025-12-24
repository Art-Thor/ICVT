import json
import os
import uuid
import time
import urllib.parse
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
import re

from openai import OpenAI

REGION = os.getenv("AWS_REGION", "ap-southeast-2")
S3 = boto3.client("s3", region_name=REGION)
TEXTRACT = boto3.client("textract", region_name=REGION)
SECRETS = boto3.client("secretsmanager", region_name=REGION)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_SECRET_NAME = os.getenv("OPENAI_SECRET_NAME", "openai/bronze-bucket-file-reader/api-key")

DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
TS_PREFIX_RE = re.compile(r"^(\d{10,13})")

_cached_openai_api_key = None


def get_openai_api_key() -> str:
    """Fetch OpenAI API key from env or Secrets Manager; cache between invocations."""
    global _cached_openai_api_key

    if os.getenv("OPENAI_API_KEY"):
        return os.getenv("OPENAI_API_KEY")

    if _cached_openai_api_key:
        return _cached_openai_api_key

    try:
        secret_value = SECRETS.get_secret_value(SecretId=OPENAI_SECRET_NAME)
    except ClientError as exc:
        raise RuntimeError(f"Failed to load OpenAI API key from secret '{OPENAI_SECRET_NAME}': {exc}") from exc

    secret_string = secret_value.get("SecretString", "")
    key = ""
    if secret_string:
        try:
            payload = json.loads(secret_string)
            key = payload.get("OPENAI_API_KEY") or payload.get("openai_api_key") or payload.get("api_key")
        except json.JSONDecodeError:
            key = secret_string

    if not key:
        raise RuntimeError(f"OpenAI API key not found in secret '{OPENAI_SECRET_NAME}'")

    _cached_openai_api_key = key
    return key


def extract_date_parts_from_filename(filename: str):
    # 1) пробуем таймстемп в начале имени (10 или 13 цифр)
    ts_match = TS_PREFIX_RE.match(filename)
    if ts_match:
        raw_ts = ts_match.group(1)
        try:
            ts_int = int(raw_ts)
            if len(raw_ts) == 13:
                ts_int = ts_int / 1000.0
            dt = datetime.utcfromtimestamp(ts_int)
            return (dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"))
        except Exception:
            pass

    # 2) иначе ищем первую YYYY-MM-DD подстроку
    match = DATE_RE.search(filename)
    if not match:
        return ("unknown", "unknown", "unknown")
    return match.group(1), match.group(2), match.group(3)


def build_dated_prefix(folder: str, filename: str):
    year, month, day = extract_date_parts_from_filename(filename)
    parts = []
    if folder:
        parts.append(folder.strip("/"))
    parts.extend([year, month, day])
    return "/".join(filter(None, parts))

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze-bucket-icvt")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver-bucket-icvt")
JOB_CONFIG_BUCKET = os.getenv("JOB_CONFIG_BUCKET") or SILVER_BUCKET
JOB_CONFIG_PREFIX = os.getenv("JOB_CONFIG_PREFIX", "portal/config/jobs").strip("/")
JOB_CONFIG_CACHE_TTL_S = int(os.getenv("JOB_CONFIG_CACHE_TTL_S", "60"))

TEXTRACT_POLL_INITIAL_DELAY = float(os.getenv("TEXTRACT_POLL_INITIAL_DELAY", "1.0"))   
TEXTRACT_POLL_MAX_DELAY = float(os.getenv("TEXTRACT_POLL_MAX_DELAY", "6.0"))           
TEXTRACT_POLL_MAX_WAIT = float(os.getenv("TEXTRACT_POLL_MAX_WAIT", "240.0"))           

_job_config_cache = {}  # job_prefix -> {"ts": float, "config": dict}

NZ_BANK_ACCOUNT_RE = re.compile(r"\b(\d{2})[- ]?(\d{4})[- ]?(\d{7})[- ]?(\d{2})\b")
AU_BSB_ACCOUNT_RE = re.compile(r"\b(\d{3})[- ]?(\d{3})\b.*?\b(\d{6,10})\b")


def job_config_s3_key(job_prefix: str) -> str:
    base = JOB_CONFIG_PREFIX.strip("/")
    job_prefix = (job_prefix or "").strip("/")
    if not job_prefix:
        return ""
    if base:
        return f"{base}/{job_prefix}.json"
    return f"{job_prefix}.json"


def get_job_config(job_prefix: str) -> dict:
    job_prefix = (job_prefix or "").strip("/")
    if not job_prefix:
        return {}
    cached = _job_config_cache.get(job_prefix)
    now = time.time()
    if cached and (now - cached.get("ts", 0) < JOB_CONFIG_CACHE_TTL_S):
        return cached.get("config") or {}
    key = job_config_s3_key(job_prefix)
    cfg = {}
    try:
        obj = S3.get_object(Bucket=JOB_CONFIG_BUCKET, Key=key)
        cfg = json.loads(obj["Body"].read())
        if not isinstance(cfg, dict):
            cfg = {}
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    _job_config_cache[job_prefix] = {"ts": now, "config": cfg}
    return cfg


def _extract_bank_account_candidate(forms_data: list[dict]):
    best = None  # (score, formatted, form_item)

    for item in forms_data or []:
        key = (item.get("key") or "").strip().lower()
        val = (item.get("value") or "").strip()
        if not val:
            continue
        score = 0
        if any(token in key for token in ("bank", "account", "acct", "bsb", "payment", "remit")):
            score += 5

        m = NZ_BANK_ACCOUNT_RE.search(val)
        if m:
            formatted = f"{m.group(1)}-{m.group(2)}-{m.group(3)}-{m.group(4)}"
            score += 10
            cand = (score, formatted, item)
            if not best or cand[0] > best[0] or (cand[0] == best[0] and len(cand[1]) > len(best[1])):
                best = cand
            continue

        digits = re.sub(r"\D+", "", val)
        if digits:
            m2 = re.search(r"(\d{15})", digits)
            if m2:
                d = m2.group(1)
                formatted = f"{d[0:2]}-{d[2:6]}-{d[6:13]}-{d[13:15]}"
                score += 8
                cand = (score, formatted, item)
                if not best or cand[0] > best[0] or (cand[0] == best[0] and len(cand[1]) > len(best[1])):
                    best = cand
                continue

        m3 = AU_BSB_ACCOUNT_RE.search(val)
        if m3:
            formatted = f"{m3.group(1)}-{m3.group(2)} {m3.group(3)}"
            score += 7
            cand = (score, formatted, item)
            if not best or cand[0] > best[0] or (cand[0] == best[0] and len(cand[1]) > len(best[1])):
                best = cand

    if not best:
        return None

    _, value, item = best
    textract_conf = (item.get("confidence") or 0) / 100.0
    return {
        "value": value,
        "original_key": item.get("original_key"),
        "value_box": item.get("value_box") or {},
        "confidence": textract_conf,
    }


def _compute_doc_confidence(normalized_kv: list[dict], required_fields: list[str], strategy: str) -> float:
    field_map = {str((i.get("key") or "")).strip().lower(): i for i in (normalized_kv or []) if i.get("key")}
    confs = []
    for name in required_fields:
        item = field_map.get(str(name).strip().lower())
        try:
            conf = float(item.get("confidence")) if item else 0.0
        except Exception:
            conf = 0.0
        confs.append(conf)
    if not confs:
        return 0.0
    if strategy == "avg_required":
        return sum(confs) / max(1, len(confs))
    return min(confs)


def lambda_handler(event, context):
    print(f"Lambda triggered with event: {json.dumps(event, default=str)}")

    if "Records" in event:
        results = []
        for record in event["Records"]:
            if "s3" in record:
                bucket_name = record["s3"]["bucket"]["name"]
                file_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

                if file_key.lower().endswith((".pdf", ".tiff", ".tif", ".png", ".jpg", ".jpeg")):
                    result = process_document_to_silver(bucket_name, file_key, SILVER_BUCKET)
                    results.append(result)
                else:
                    results.append({
                        "file_key": file_key,
                        "status": "skipped",
                        "reason": "Unsupported extension"
                    })

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Processed {len(results)} files",
                "results": results
            }, ensure_ascii=False)
        }

    else:
        file_key = event.get("file_key")
        source_bucket = event.get("bucket", BRONZE_BUCKET)

        if not file_key:
            return {"statusCode": 400, "body": json.dumps({"error": "Provide file_key"}, ensure_ascii=False)}

        result = process_document_to_silver(source_bucket, file_key, SILVER_BUCKET)
        return {"statusCode": 200, "body": json.dumps(result, ensure_ascii=False, default=str)}



def deduplicate_by_confidence(items):
    unique = {}
    for item in items:
        k = (item.get("key") or "").strip().lower()
        if not k:
            k = f"__orig__:{item.get('original_key')}"
        if k not in unique or (item.get("confidence", 0) or 0) > (unique[k].get("confidence", 0) or 0):
            unique[k] = item
    return list(unique.values())


def is_pdf_or_tiff(key: str) -> bool:
    k = key.lower()
    return k.endswith(".pdf") or k.endswith(".tiff") or k.endswith(".tif")


def is_image(key: str) -> bool:
    k = key.lower()
    return k.endswith(".png") or k.endswith(".jpg") or k.endswith(".jpeg")


def head_object_or_error(bucket: str, key: str):
    try:
        S3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise FileNotFoundError(f"File not found: s3://{bucket}/{key}")
        raise


def textract_analyze_sync(bucket: str, key: str) -> dict:
    return TEXTRACT.analyze_document(
        Document={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["TABLES", "FORMS"]
    )


def textract_analyze_async(bucket: str, key: str) -> dict:
    start = TEXTRACT.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["TABLES", "FORMS"]
    )
    job_id = start["JobId"]
    print(f"Textract async job started: {job_id}")

    delay = TEXTRACT_POLL_INITIAL_DELAY
    waited = 0.0

    pages = []
    next_token = None
    status_seen = None

    while True:
        if waited > TEXTRACT_POLL_MAX_WAIT:
            raise TimeoutError(f"Textract job {job_id} exceeded maximum wait of {TEXTRACT_POLL_MAX_WAIT}s")

        if next_token:
            resp = TEXTRACT.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            resp = TEXTRACT.get_document_analysis(JobId=job_id)

        status = resp.get("JobStatus")
        status_seen = status
        print(f"Textract job {job_id} status: {status}")

        if status == "FAILED":
            raise RuntimeError(f"Textract job failed: {resp.get('StatusMessage')}")

        if "Blocks" in resp:
            pages.append(resp)

        next_token = resp.get("NextToken")

        if not next_token and status == "SUCCEEDED":
            break

        time.sleep(delay)
        waited += delay
        delay = min(delay * 1.5, TEXTRACT_POLL_MAX_DELAY)

    merged = {"Blocks": []}
    for p in pages:
        merged["Blocks"].extend(p.get("Blocks", []))
    return merged


def parse_kv_from_blocks(analyze_response: dict):
    key_map, value_map, block_map = {}, {}, {}
    for block in analyze_response.get("Blocks", []):
        block_map[block["Id"]] = block
        if block["BlockType"] == "KEY_VALUE_SET":
            if "KEY" in block.get("EntityTypes", []):
                key_map[block["Id"]] = block
            else:
                value_map[block["Id"]] = block

    forms_data = []
    for key_id in key_map:
        value_id, key_text, value_text = None, "", ""
        key_box, val_box = {}, {}
        conf = key_map[key_id].get("Confidence", 0)

        for rel in key_map[key_id].get("Relationships", []):
            if rel["Type"] == "CHILD":
                for child_id in rel["Ids"]:
                    if child_id in block_map and block_map[child_id]["BlockType"] == "WORD":
                        key_text += block_map[child_id].get("Text", "") + " "
            elif rel["Type"] == "VALUE":
                value_id = rel["Ids"][0] if rel["Ids"] else None

        if "Geometry" in key_map[key_id]:
            key_box = key_map[key_id]["Geometry"]["BoundingBox"]

        if value_id and value_id in value_map:
            for rel in value_map[value_id].get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for child_id in rel["Ids"]:
                        if child_id in block_map and block_map[child_id]["BlockType"] == "WORD":
                            value_text += block_map[child_id].get("Text", "") + " "
            if "Geometry" in value_map[value_id]:
                val_box = value_map[value_id]["Geometry"]["BoundingBox"]

        if key_text.strip():
            forms_data.append({
                "original_key": str(uuid.uuid4()),
                "key": key_text.strip(),
                "value": value_text.strip(),
                "confidence": conf,
                "key_box": key_box,
                "value_box": val_box
            })
    return forms_data


def normalize_with_gpt(forms_data):
    gpt_input = [{"original_key": f["original_key"], "key": f["key"], "value": f["value"]} for f in forms_data]
    print("Sending forms data to GPT for normalization...")
    print("gpt_input", gpt_input)

    prompt = f"""
You are a data normalization model working with key-value pairs extracted from invoices via AWS Textract.

Your task:
Return a **JSON array** of objects, where each object represents a standardized field and contains:
- "original_key": the UUID from the input
- "field_name": one of the target standardized fields
- "value": the normalized textual value
- "gptConfidence": a numeric confidence score between 0.0 and 1.0 reflecting how confident you are in your mapping

Your output MUST strictly follow this JSON format:
[
{{
    "original_key": "...",
    "field_name": "Invoice Number",
    "value": "...",
    "gptConfidence": 0.95
}},
...
]

---

### Target standardized fields to detect:
1. **GST Number** — look for fields with "GST No", "GST Number".
2. **Invoice Date**
3. **Total Amount** — must correspond to the **total including GST**, not subtotal or other totals.
- Prefer fields containing "Total", "Incl", or "Total NZD Incl. GST".
4. **GST Amount** — tax value usually around 10–20% of total; 
- may appear near keys like "15%", "GST", or "Tax".
- if both a key with "%" and a numeric value exist, this is the GST Amount.
5. **Invoice Number** — look for fields with "Invoice No", "Invoice #", or "Invoice Number".
6. **Supplier/Vendor Name** — look for fields with "Account Name", "Beneficiary Name", "EFT Payments";
— detect the company name even if not explicitly labeled; 
- analyze the context of all text (e.g., addresses, contact info, phone numbers, email domains);
- choose the most probable supplier name (can be also Account Name)
7. **Account Number** — look for fields with "Acct", "Account Number", "Acc Number".
8. **ABN number** — look for fields with "ABN", "ABN Number".
9. **PO Number** — look for fields with "order No", "order Number".
10. **Creditor Name** — same as Supplier/Vendor Name.
11. **Creditor Code** — make Code from Creditor Name.
12. **Packing/Deliver/Con number** — look for fields with "Packing #", "Deliver #", "Con #", "Packing Number", "Packing No", "Deliver Number", "Deliver No", "Con Number", "Con No", "Co Number".
13. **Invoice Total** — same as Total Amount.

---

### Additional rules:
- If unsure about a field, output with "value": "" and "gptConfidence": 0.0, but always return JSON array.
- Include all fields listed.
- Do not include any fields beyond listed.
- Preserve the "original_key" from the input.

### Input data:
{json.dumps(gpt_input, ensure_ascii=False)}

Return only the final JSON array as valid JSON.
""".strip()

    openai_client = OpenAI(api_key=get_openai_api_key())
    gpt_response = openai_client.responses.create(
        model=OPENAI_MODEL,
        input=[{"role": "user", "content": prompt}],
        temperature=0
    )

    usage_obj = getattr(gpt_response, "usage", None)
    usage = {}
    if usage_obj:
        for k in ("input_tokens", "output_tokens", "total_tokens"):
            v = getattr(usage_obj, k, None)
            if v is not None:
                usage[k] = v

    output_text = getattr(gpt_response, "output_text", "") or ""
    output_text = output_text.strip()
    if output_text.startswith("```"):
        output_text = output_text.split("```", 2)[-1]
    if output_text.endswith("```"):
        output_text = output_text.rsplit("```", 1)[0]

    print("=== GPT raw output ===")
    print(output_text)

    try:
        normalized_array = json.loads(output_text)
        if not isinstance(normalized_array, list):
            raise ValueError("GPT output is not a JSON array")
    except Exception as e:
        print("GPT output parse error:", str(e))
        normalized_array = []

    final_kv = []
    for item in normalized_array:
        orig_id = item.get("original_key")
        match = next((f for f in forms_data if f["original_key"] == orig_id), None)

        textract_confidence = (match["confidence"] / 100.0) if match and match.get("confidence") is not None else 0
        gpt_confidence = item.get("gptConfidence", 1.0)
        try:
            gpt_confidence = float(gpt_confidence)
        except Exception:
            gpt_confidence = 0.0

        combined_confidence = textract_confidence * gpt_confidence
        bbox = match.get("value_box") if match else {}

        if combined_confidence >= 0.9:
            color = "#2E7D32"  # green
        elif combined_confidence >= 0.7:
            color = "#F9A825"  # yellow
        else:
            color = "#C62828"  # red

        final_kv.append({
            "original_key": orig_id,
            "key": item.get("field_name"),
            "value": item.get("value", ""),
            "confidence": combined_confidence,
            "color": color,
            "BoundingBox": bbox
        })

    final_kv = deduplicate_by_confidence(final_kv)
    return final_kv, usage


def process_document_to_silver(source_bucket: str, file_key: str, silver_bucket: str) -> dict:
    print(f"Processing document: s3://{source_bucket}/{file_key}")

    try:
        head_object_or_error(source_bucket, file_key)
        '''if is_pdf_or_tiff(file_key):
            analyze_response = textract_analyze_async(source_bucket, file_key)
        elif is_image(file_key):
            analyze_response = textract_analyze_sync(source_bucket, file_key)
        else:
            # Допустим, что PDF без расширения или иной формат – пробуем асинхронку,
            # а в случае ошибки вернём понятное сообщение
            analyze_response = textract_analyze_async(source_bucket, file_key)'''
        analyze_response = textract_analyze_async(source_bucket, file_key)
        forms_data = parse_kv_from_blocks(analyze_response)

        normalized_kv, openai_usage = normalize_with_gpt(forms_data)

        # Prefer extracting a real bank account number for "Account Number" when it is present.
        acct = _extract_bank_account_candidate(forms_data)
        if acct and acct.get("value"):
            found = False
            for item in normalized_kv:
                if (item.get("key") or "").strip().lower() == "account number":
                    item["value"] = acct["value"]
                    try:
                        item["confidence"] = max(float(item.get("confidence") or 0.0), float(acct.get("confidence") or 0.0))
                    except Exception:
                        pass
                    if acct.get("value_box"):
                        item["BoundingBox"] = acct["value_box"]
                    found = True
                    break
            if not found:
                normalized_kv.append(
                    {
                        "original_key": acct.get("original_key"),
                        "key": "Account Number",
                        "value": acct["value"],
                        "confidence": float(acct.get("confidence") or 0.0),
                        "BoundingBox": acct.get("value_box") or {},
                    }
                )

        folder_name = os.path.dirname(file_key)
        job_prefix = folder_name.strip("/")
        cfg = get_job_config(job_prefix) or {}

        auto_cfg = cfg.get("auto_processing") if isinstance(cfg.get("auto_processing"), dict) else {}
        approve_threshold = auto_cfg.get("approve_min_confidence") or auto_cfg.get("approve_threshold") or cfg.get("approve_min_confidence") or cfg.get("auto_approve_threshold")
        hold_threshold = auto_cfg.get("hold_below_confidence") or auto_cfg.get("hold_below")
        strategy = str(auto_cfg.get("strategy") or "min_required").strip().lower()

        required_fields = cfg.get("required_fields")
        if not isinstance(required_fields, list):
            required_fields = ["Invoice Number", "Invoice Date", "Total Amount", "Supplier/Vendor Name"]

        doc_conf = _compute_doc_confidence(normalized_kv, [str(x) for x in required_fields], strategy)
        status = "To Review"
        decision = None
        try:
            approve_threshold = float(approve_threshold) if approve_threshold is not None else None
        except Exception:
            approve_threshold = None
        try:
            hold_threshold = float(hold_threshold) if hold_threshold is not None else None
        except Exception:
            hold_threshold = None

        if approve_threshold is not None:
            if doc_conf >= approve_threshold:
                status = "Approved"
                decision = "Approved"
            elif hold_threshold is not None and doc_conf < hold_threshold:
                status = "Hold"
                decision = "Hold"

        page_count = sum(1 for b in (analyze_response.get("Blocks") or []) if b.get("BlockType") == "PAGE")

        result = {
            "source_bucket": source_bucket,
            "source_file": file_key,
            "processed_at": datetime.utcnow().isoformat(),
            "processing_id": str(uuid.uuid4()),
            "normalized_data": normalized_kv,
            "status": status,
            "page_count": page_count,
            "openai": {"model": OPENAI_MODEL, "usage": openai_usage},
            "auto_processing": {
                "strategy": strategy,
                "required_fields": required_fields,
                "doc_confidence": doc_conf,
                "approve_threshold": approve_threshold,
                "hold_threshold": hold_threshold,
                "decision": decision,
            },
        }

        clean_filename = os.path.splitext(os.path.basename(file_key))[0]
        dated_prefix = build_dated_prefix(folder_name, clean_filename)
        output_key = f"textract-results/{dated_prefix}/{clean_filename}.json"

        S3.put_object(
            Bucket=silver_bucket,
            Key=output_key,
            Body=json.dumps(result, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )

        return {
            "file_key": file_key,
            "status": "completed",
            "output_bucket": silver_bucket,
            "output_key": output_key,
            "processing_id": result["processing_id"]
        }

    except Exception as e:
        print(f"Error processing document: {str(e)}")
        return {
            "file_key": file_key,
            "status": "error",
            "error": str(e),
            "processing_id": str(uuid.uuid4())
        }
