import json
import boto3
import zipfile
import io
import csv
import os
import re
from datetime import datetime

s3 = boto3.client('s3')

DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
TS_PREFIX_RE = re.compile(r"^(\d{10,13})")


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
        parts.append(str(folder).strip("/"))
    parts.extend([year, month, day])
    return "/".join(filter(None, parts))

def lambda_handler(event, context):
    try:
        print("Event received:", event)
        bronze_bucket = os.getenv('BRONZE_BUCKET', 'bronze-bucket-icvt')
        silver_bucket = os.getenv('SILVER_BUCKET', 'silver-bucket-icvt')
        export_prefix = os.getenv('EXPORT_PREFIX', 'export')
        textract_prefix = os.getenv('TEXTRACT_PREFIX', 'textract-results')
        auth_token = os.getenv('EXPORT_AUTH_TOKEN')

        if isinstance(event, str):
            event = json.loads(event)

        body = event.get('body')
        if isinstance(body, str):
            body = json.loads(body)

        print("Parsed body:", body)

        filename = body.get('filename')
        json_data = body.get('json_data')
        folder = body.get('folder')
        headers = {k.lower(): v for k, v in (event.get('headers') or {}).items()} if isinstance(event, dict) else {}
        if auth_token:
            token_val = headers.get('authorization') or headers.get('x-api-key') or headers.get('x-export-token')
            if token_val and token_val.lower().startswith('bearer '):
                token_val = token_val.split(' ', 1)[1]
            if token_val != auth_token:
                return {
                    'statusCode': 401,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'Unauthorized'})
                }

        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except Exception:
                pass  

        print("Filename:", filename)
        print("JSON data keys:", list(json_data.keys()) if isinstance(json_data, dict) else type(json_data))

        if not filename or not json_data:
            raise ValueError("Missing 'filename' or 'json_data' in request body")

        # ---------- 1. Загружаем исходный файл ----------
        key = f"{folder}/{filename}" if folder else filename
        print(f"Downloading from bronze: {bronze_bucket}/{key}")

        original_obj = s3.get_object(Bucket=bronze_bucket, Key=key)
        original_data = original_obj['Body'].read()

        # ---------- 2. Создаём CSV ----------
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(['key', 'value'])
        if isinstance(json_data, dict):
            for k, v in json_data.items():
                csv_writer.writerow([k, v])
        elif isinstance(json_data, list):
            for item in json_data:
                csv_writer.writerow([item.get('key'), item.get('value')])
        csv_data = csv_buffer.getvalue()

        # ---------- 3. Создаём ZIP ----------
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(filename, original_data)
            zip_file.writestr('data.csv', csv_data)

        zip_key = filename.rsplit('.', 1)[0] + '.zip'
        zip_buffer.seek(0)

        dated_prefix = build_dated_prefix(folder, filename)
        export_key = f"{export_prefix}/{dated_prefix}/{zip_key}"
        s3.put_object(Bucket=silver_bucket, Key=export_key, Body=zip_buffer.getvalue())
        print(f"ZIP uploaded: {silver_bucket}/{export_key}")

        # ---------- 4. Обновляем JSON в textract-results ----------
        base_name = os.path.splitext(filename)[0]
        dated_textract_prefix = build_dated_prefix(folder, filename)
        new_textract_key = f"{textract_prefix}/{dated_textract_prefix}/{base_name}.json"
        legacy_textract_key = f"{textract_prefix}/{folder}/{base_name}.json" if folder else f"{textract_prefix}/{base_name}.json"
        textract_candidates = [new_textract_key]
        if legacy_textract_key != new_textract_key:
            textract_candidates.append(legacy_textract_key)

        textract_key = None
        textract_data = None
        for candidate in textract_candidates:
            try:
                textract_obj = s3.get_object(Bucket=silver_bucket, Key=candidate)
                textract_key = candidate
                textract_data = json.loads(textract_obj['Body'].read().decode('utf-8'))
                print(f"Found Textract JSON at: {candidate}")
                break
            except s3.exceptions.NoSuchKey:
                continue

        if textract_key and isinstance(textract_data, dict):
            try:
                textract_data["status"] = "Exported"
                print("Updated status to Exported")

                s3.put_object(
                    Bucket=silver_bucket,
                    Key=textract_key,
                    Body=json.dumps(textract_data, ensure_ascii=False, indent=2).encode('utf-8'),
                    ContentType='application/json'
                )
                print(f"Updated Textract JSON: {silver_bucket}/{textract_key}")
            except Exception as e:
                print(f"⚠️ Failed to update Textract JSON: {str(e)}")
        else:
            print(f"⚠️ Textract JSON not found for {filename} in any expected path")

        # ---------- 5. Возвращаем ответ ----------
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': f"ZIP created and uploaded to {silver_bucket}/{export_key}",
                'updated_json': textract_key
            })
        }

    except Exception as e:
        print("Error occurred:", str(e))
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }
