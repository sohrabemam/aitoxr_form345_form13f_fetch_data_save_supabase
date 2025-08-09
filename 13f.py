import os
import time
import traceback
import requests
import xmltodict
import boto3
import psycopg
from decimal import Decimal
from dotenv import load_dotenv
from botocore.client import Config

load_dotenv()

PG_CONN = psycopg.connect(
    host=os.environ['PG_HOST'],
    port=os.environ['PG_PORT'],
    dbname=os.environ['PG_DATABASE'],
    user=os.environ['PG_USER'],
    password=os.environ['PG_PASSWORD'],
    autocommit=True
)

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['S3_ENDPOINT'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY'],
    region_name=os.environ['S3_REGION'],
    config=Config(signature_version='s3v4')
)

BUCKET = os.environ['S3_BUCKET']
USER_AGENT = "13F Downloader (guddu.kumar@aitoxr.com)"

def extract_xml_sections(txt):
    import re
    return [m.strip() for m in re.findall(r"<XML>([\s\S]*?)</XML>", txt, re.IGNORECASE)]

def format_date(value):
    if isinstance(value, (int, float)):
        raise ValueError(f"Invalid date type: {value} (number)")

    if isinstance(value, str):
        value = value.strip()
        parts = value.replace("/", "-").split("-")
        if len(parts) == 3:
            y, m, d = parts if len(parts[0]) == 4 else (parts[2], parts[0], parts[1])
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        raise ValueError(f"Invalid date string: {value}")

    elif hasattr(value, 'isoformat'):
        return value.isoformat()

    raise ValueError(f"Unsupported date value: {value}")

def to_char1(value):
    if str(value).lower() == 'true':
        return 'Y'
    elif str(value).lower() == 'false':
        return 'N'
    return None

def safe_str(val):
    return val.strip() if isinstance(val, str) else str(val)

def insert_one(cur, table, cols, vals):
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
    cur.execute(sql, vals)

def insert_many(cur, table, cols, rows):
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
    cur.executemany(sql, rows)

def main():
    with PG_CONN.cursor() as cur:
        cur.execute("SELECT accession_number, cik, filing_date, xbrl_url FROM pipelines.sec13f_jobs WHERE status = 'PENDING'")
        jobs = cur.fetchall()
        print(f"📋 Total pending jobs: {len(jobs)}")

        for accession_number, cik, filing_date, xbrl_url in jobs:
            cik = cik.zfill(10)
            year = str(filing_date.year)
            txt_path = f"thirteen_f/txt/{year}/{accession_number}.txt"
            print(f"\n🔄 Processing {accession_number}")
            try:
                response = requests.get(xbrl_url, headers={"User-Agent": USER_AGENT})
                response.raise_for_status()
                txt = response.text
                print("📄 Downloaded")
                s3.put_object(Bucket=BUCKET, Key=txt_path, Body=txt.encode("utf-8"))
                print("☁️ Uploaded to S3")
                xmls = extract_xml_sections(txt)
                print(f"📦 Found {len(xmls)} XML blocks")
                state = {
                    "submission": None, "cover": None, "summary": None,
                    "signature": None, "other1": [], "other2": [], "infotbl": []
                }

                for xml in xmls:
                    parsed = xmltodict.parse(xml)
                    doc = parsed.get("XML") or parsed
                    if "edgarSubmission" in doc:
                        hd, fd = doc["edgarSubmission"].get("headerData", {}), doc["edgarSubmission"].get("formData", {})
                        period = format_date(hd["filerInfo"]["periodOfReport"])
                        state["submission"] = ("sec_13f.submission", [
                            "accession_number", "cik", "filing_date", "period_of_report", "submission_type"
                        ], [accession_number, cik, format_date(filing_date), period, hd["submissionType"]])

                        cp = fd.get("coverPage")
                        if cp:
                            state["cover"] = ("sec_13f.coverpage", [
                                "accession_number", "filing_manager_name", "report_calendar_or_quarter", "is_amendment", "amendment_type", "report_type"
                            ], [accession_number, cp.get("filingManager", {}).get("name", ""), format_date(cp.get("reportCalendarOrQuarter")), to_char1(cp.get("isAmendment")), cp.get("amendmentNo"), cp["reportType"]])

                        sp = fd.get("summaryPage")
                        if sp:
                            table_entry_total = int(sp["tableEntryTotal"]) if sp.get("tableEntryTotal") else None
                            table_value_total = Decimal(sp["tableValueTotal"]) if sp.get("tableValueTotal") else None
                            state["summary"] = ("sec_13f.summarypage", [
                                "accession_number", "table_entry_total", "table_value_total"
                            ], [accession_number, table_entry_total, table_value_total])

                        sb = fd.get("signatureBlock")
                        if sb:
                            state["signature"] = ("sec_13f.signature", [
                                "accession_number", "name", "title"
                            ], [accession_number, sb["signature"], sb["title"]])

                        for row in fd.get("otherManagersInfo", {}).get("otherManager", []):
                            state["other1"].append((accession_number, row.get("cik"), row.get("name", "")))

                        for row in fd.get("otherManagers2Info", {}).get("otherManager2", []):
                            state["other2"].append((accession_number, row.get("cik"), row.get("name", "")))

                    elif "informationTable" in doc:
                        period = state["submission"][2][3]
                        rows = doc["informationTable"].get("infoTable", [])
                        if not isinstance(rows, list): rows = [rows]
                        for row in rows:
                            state["infotbl"].append((
                                accession_number, period, row["nameOfIssuer"], row.get("titleOfClass"), row["cusip"],
                                Decimal(row["value"]), Decimal(row["shrsOrPrnAmt"]["sshPrnamt"]),
                                row["shrsOrPrnAmt"]["sshPrnamtType"], row.get("putCall"),
                                row["investmentDiscretion"],
                                Decimal(row.get("votingAuthority", {}).get("Sole", 0)),
                                Decimal(row.get("votingAuthority", {}).get("Shared", 0)),
                                Decimal(row.get("votingAuthority", {}).get("None", 0))
                            ))

                if state["submission"]: insert_one(cur, *state["submission"])
                if state["cover"]: insert_one(cur, *state["cover"])
                if state["summary"]: insert_one(cur, *state["summary"])
                if state["signature"]: insert_one(cur, *state["signature"])
                if state["other1"]:
                    insert_many(cur, "sec_13f.other_manager", ["accession_number", "cik", "name"], state["other1"])
                if state["other2"]:
                    insert_many(cur, "sec_13f.other_manager2", ["accession_number", "cik", "name"], state["other2"])
                if state["infotbl"]:
                    insert_many(cur, "sec_13f.infotable", [
                        "accession_number", "period_of_report", "nameof_issuer", "title_of_class", "cusip",
                        "value", "ssh_prn_amt", "ssh_prn_amt_type", "put_call", "investment_discretion",
                        "voting_auth_sole", "voting_auth_shared", "voting_auth_none"
                    ], state["infotbl"])

                cur.execute("UPDATE pipelines.sec13f_jobs SET status='SUCCESS', error_message=NULL, last_attempted_at=NOW() WHERE accession_number=%s", [accession_number])
                print(f"✅ Job {accession_number} marked SUCCESS")

            except Exception as e:
                print(f"❌ Error processing {accession_number}: {e}")
                traceback.print_exc()
                cur.execute("UPDATE pipelines.sec13f_jobs SET status='FAILED', error_message=%s, last_attempted_at=NOW() WHERE accession_number=%s", [str(e), accession_number])

if __name__ == "__main__":
    main()
