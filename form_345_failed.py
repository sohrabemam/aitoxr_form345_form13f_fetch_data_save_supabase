import os
import time
import re
import traceback
import requests
import xmltodict
import boto3
import psycopg
from decimal import Decimal
from dotenv import load_dotenv
from botocore.client import Config
import uuid
import pandas as pd
from collections import defaultdict
from datetime import datetime, date
import requests


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
   
    return [m.strip() for m in re.findall(r"<XML>([\s\S]*?)</XML>", txt, re.IGNORECASE)]

def extract_sec_file_number(txt):
    match = re.search(r'SEC FILE NUMBER:\s+([0-9\-]+)', txt)
    return match.group(1).strip() if match else None


from datetime import datetime, date
import re

def format_date(value):
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    value = re.sub(r'(T.*)?([-+]\d{2}:\d{2})$', '', value)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).date().strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Invalid date string: {value}")
def to_char1(value):
    if str(value).lower() == 'true':
        return 'Y'
    elif str(value).lower() == 'false':
        return 'N'
    return None
def safe_str(val):
    return val.strip() if isinstance(val, str) else str(val)

def insert_one(cur, table, cols, vals):
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))}) on conflict do nothing"
    cur.execute(sql, vals)
def insert_many(cur, table_name, columns, rows):
    try:
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) on conflict do nothing"
        for row in rows:
            try:
                cur.execute(sql, row)
            except Exception as e:
                print(f"❌ Error inserting into {table_name} with row: {row}")
                raise
    except Exception as e:
        raise e
    
#def insert_many(cur, table, columns, rows):
#    placeholders = ", ".join(["%s"] * len(columns))
#    col_names = ", ".join(columns)
#    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) on conflict do nothing"
#    cur.executemany(sql, rows)
def main():
    with PG_CONN.cursor() as cur:
        cur.execute("""
            SELECT accession_number, cik, filing_date, xbrl_url
            FROM pipelines.form_345_jobs
            WHERE status = 'FAILED'
        """)
        jobs = cur.fetchall()
        print(f"📋 Total pending jobs: {len(jobs)}")
        for accession_number, cik, filing_date, xbrl_url in jobs:
            cik = cik.zfill(10)
            year = str(filing_date.year)
            txt_path = f"form_345/txt/{year}/{accession_number}.txt"
            try:
                response = requests.get(xbrl_url, headers={"User-Agent": USER_AGENT})
                response.raise_for_status()
                txt = response.text
                s3.put_object(Bucket=BUCKET, Key=txt_path, Body=txt.encode("utf-8"))
                xmls = extract_xml_sections(txt)
                file_number = extract_sec_file_number(txt)
                state = {
                    "insider_submission_dimension": None, "reporting_owner": [], "insider_owner_signature_fact": None,
                    "holding_non_derivative": [], "holding_derivative": [], "transaction_derivative": [], "transaction_non_derivative": [],
                    "footnote_dimension":[]
                }
                for xml in xmls:
                    parsed = xmltodict.parse(xml)
                    doc = parsed.get("XML") or parsed
                    if "ownershipDocument" in doc:
                        sb = doc["ownershipDocument"]
                        period = format_date(sb.get("periodOfReport",""))
                        date_of_original_submission = format_date(sb.get("dateOfOriginalSubmission")) if sb.get("dateOfOriginalSubmission") else None
                        no_securities_owned = sb.get("noSecuritiesOwned",None)
                        no_sub_sec16 = sb.get("notSubjectToSection16",None)
                        form3_holding_reported = sb.get("form3HoldingReported",None)
                        form4_transactions_reported = sb.get("form4TransactionsReported",None)
                        document_type = sb.get("documentType","")
                        issuer_cik = sb.get("issuer",{}).get("issuerCik","")
                        issuer_name = sb.get("issuer",{}).get("issuerName","")
                        issuer_trading_symbol = sb.get("issuer",{}).get("issuerTradingSymbol","")
                        remarks = sb.get("remarks","")
                        state["insider_submission_dimension"] = ("insider_trades.insider_submission_dimension", [
                            "accession_number", "filing_date", "period_of_report","date_of_original_submission","no_securities_owned","not_subject_sec16","form3_holdings_reported","form4_transactions_reported","document_type", "issuer_cik","issuer_name","issuer_trading_symbol", "remarks"
                        ], [accession_number, format_date(filing_date), period,date_of_original_submission,no_securities_owned,no_sub_sec16,form3_holding_reported, form4_transactions_reported, document_type, issuer_cik, issuer_name, issuer_trading_symbol, remarks ])
                        reporting_owners = sb.get("reportingOwner", [])
                        if isinstance(reporting_owners, dict):  # if only one reportingOwner, make it a list
                            if not isinstance(reporting_owners, list):
                              reporting_owners = [reporting_owners]
                        for owner in reporting_owners:
                            rpo_id = owner.get("reportingOwnerId", {})
                            rpo_add = owner.get("reportingOwnerAddress", {})
                            rpo_rel = owner.get("reportingOwnerRelationship", {})
                            relationship_types = []
                            if rpo_rel.get("isDirector", "").lower() in ["1", "true"]:
                                relationship_types.append("Director")
                            if rpo_rel.get("isOfficer", "").lower() in ["1", "true"]:
                                relationship_types.append("Officer")
                            if rpo_rel.get("isTenPercentOwner", "").lower() in ["1", "true"]:
                                relationship_types.append("TenPercentOwner")
                            if rpo_rel.get("isOther", "").lower() in ["1", "true"]:
                                relationship_types.append("Other")
                            if not relationship_types:
                                raise ValueError(f"Missing relationship_type in reportingOwner block for CIK: {rpo_id.get('rptOwnerCik')}")
                            relationship_type = ",".join(relationship_types)
                            record = (
                                "insider_trades.insider_reporting_owner_dimension",
                                [
                                    "accession_number", "reporting_owner_cik", "reporting_owner_name", "relationship_type",
                                    "officer_title", "other_relationship_details", "address_street1", "address_street2",
                                    "city", "state_code", "zip_code", "state_description", "file_number"
                                ],
                                [
                                    accession_number,
                                    rpo_id.get("rptOwnerCik", None),
                                    rpo_id.get("rptOwnerName", ""),
                                    relationship_type,
                                    rpo_rel.get("officerTitle", None),
                                    rpo_rel.get("otherText", None),
                                    rpo_add.get("rptOwnerStreet1", None),
                                    rpo_add.get("rptOwnerStreet2", None),
                                    rpo_add.get("rptOwnerCity", None),
                                    rpo_add.get("rptOwnerState", None),
                                    rpo_add.get("rptOwnerZipCode", None),
                                    rpo_add.get("rptOwnerStateDescription", None),
                                    file_number
                                ]
                            )
                            state["reporting_owner"].append(record)
                        oS = doc['ownershipDocument'].get("ownerSignature", {})
                        if isinstance(oS, list):
                            oS = oS[0]
                        if oS:
                            name = oS.get("signatureName") or oS.get("SignatureName")
                            date_raw = oS.get("signatureDate") or oS.get("SignatureDate")
                            date = format_date(date_raw) if date_raw else None
                            state["insider_owner_signature_fact"] = (
                                "insider_trades.insider_owner_signature_fact",
                                ["accession_number", "owner_signature_name", "owner_signature_date"],
                                [accession_number, name, date]
                            )
                        non_derivative_table = sb.get("nonDerivativeTable",{})
                        non_derivative_holding = []
                        if isinstance(non_derivative_table, dict):
                            non_derivative_holding = non_derivative_table.get("nonDerivativeHolding", [])
                            if not isinstance(non_derivative_holding, list):
                               non_derivative_holding = [non_derivative_holding]
                        for txn in non_derivative_holding:
                            # Extract field values
                            security_title = txn.get("securityTitle", {}).get("value", "")
                            shares_owned_data = txn.get("postTransactionAmounts", {}).get("sharesOwnedFollowingTransaction", {})
                            shares_owned_str = ""
                            if isinstance(shares_owned_data, dict):
                                shares_owned_str = shares_owned_data.get("value", "")
                            price_per_share_data = txn.get("transactionAmounts", {}).get("transactionPricePerShare", {})
                            price_per_share_str = ""
                            if isinstance(price_per_share_data,dict):
                                price_per_share_str =price_per_share_data.get("value", "")

                            form_type_data = txn.get("transactionCoding",{})
                            form_type = ""
                            if isinstance(form_type_data, dict):
                                form_type = form_type_data.get("transactionFormType","")
                            
                            ownership_type_data = txn.get("ownershipNature", {}).get("directOrIndirectOwnership", {})
                            ownership_type = ""
                            if isinstance(ownership_type_data, dict):
                                ownership_type = ownership_type_data.get("value", "")
                            ownership_nature_data =txn.get("ownershipNature", {}).get("natureOfOwnership", {})
                            ownership_nature = None
                            if isinstance(ownership_nature_data, dict):
                                ownership_nature = ownership_nature_data.get("value", None)
                            try:
                                shares_owned_following = float(shares_owned_str) if shares_owned_str else None
                            except ValueError:
                                shares_owned_following = None
                            try:
                                price_per_share = float(price_per_share_str) if price_per_share_str else None
                            except ValueError:
                                price_per_share = None
                            # Calculate value owned
                            value_owned_following = (
                                shares_owned_following * price_per_share
                                if shares_owned_following is not None and price_per_share is not None
                                else None
                            )
                            holding_sk = (int(uuid.uuid4().int >> 64) % 9_990_000) + 10_000
                            record = (
                                "insider_trades.insider_holding_non_derivative_fact",
                                [
                                    "accession_number",
                                    "holding_sk",
                                    "security_title",
                                    "form_type",
                                    "shares_owned_following",
                                    "value_owned_following",
                                    "ownership_type",
                                    "ownership_nature"
                                ],
                                [
                                    accession_number,
                                    holding_sk,
                                    security_title,
                                    form_type,
                                    shares_owned_following,
                                    value_owned_following,
                                    ownership_type,
                                    ownership_nature
                                ]
                            )
                            state["holding_non_derivative"].append(record)
                            
                        derivative_holding_table = sb.get("derivativeTable",{})
                        derivative_holding = []
                        if isinstance(derivative_holding_table, dict):
                            derivative_holding = derivative_holding_table.get("derivativeHolding", [])
                            if not isinstance(derivative_holding, list):
                               derivative_holding = [derivative_holding]
                        for txn in derivative_holding:
                            security_title = txn.get("securityTitle", {}).get("value", "")
                            shares_owned_data= txn.get("postTransactionAmounts", {}).get("sharesOwnedFollowingTransaction", {})
                            shares_owned_str = ""
                            if isinstance(shares_owned_data, dict):
                                shares_owned_str = shares_owned_data.get("value", None)
                            price_per_share_data = txn.get("transactionAmounts", {}).get("transactionPricePerShare", {})
                            price_per_share_str = None
                            if isinstance(price_per_share_data, dict):
                                price_per_share_str = price_per_share_data.get("value", None)
                            form_type_data = txn.get("transactionCoding",{})
                            form_type = ""
                            if isinstance(form_type_data, dict):
                                form_type = form_type_data.get("transactionFormType","")
                            conversion_price_data = txn.get("conversionOrExercisePrice", {})
                            conversion_price = None
                            if isinstance(form_type_data, dict):
                                conversion_price= conversion_price_data.get("value", None)
                            exercise_date = format_date(txn.get("exerciseDate", {}).get("value") if isinstance(txn.get("exerciseDate"), dict) else None)
                            expiration_date = format_date(txn.get("expirationDate",{}).get("value") if isinstance(txn.get("expirationDate"), dict) else None)
                            ownership_type_data = txn.get("ownershipNature", {}).get("directOrIndirectOwnership", {})
                            ownership_type = ""
                            if isinstance(ownership_type_data, dict):
                                ownership_type = ownership_type_data.get("value", "")
                            ownership_nature_data = txn.get("ownershipNature", {}).get("natureOfOwnership", {})
                            ownership_nature = ""
                            if isinstance(ownership_nature_data, dict):
                                ownership_nature = ownership_nature_data.get("value", None)
                            underlying_security_title_data = txn.get("underlyingSecurity",{}).get("underlyingSecurityTitle",{})
                            underlying_security_title = ""
                            if isinstance(underlying_security_title_data, dict):
                                underlying_security_title = underlying_security_title_data.get("value","")
                            underlying_security_share_data = txn.get("underlyingSecurity",{}).get("underlyingSecurityShares",{})
                            underlying_security_share = None
                            if isinstance(underlying_security_share_data, dict):
                                underlying_security_share = underlying_security_share_data.get("value",None)
                            try:
                                shares_owned_following = float(shares_owned_str) if shares_owned_str else None
                            except ValueError:
                                shares_owned_following = None
                            try:
                                price_per_share = float(price_per_share_str) if price_per_share_str else None
                            except ValueError:
                                price_per_share = None
                            try:
                                underlying_security_share = float(underlying_security_share) if underlying_security_share else None
                            except ValueError:
                                underlying_security_share = None
                            value_owned_following = (
                                shares_owned_following * price_per_share
                                if shares_owned_following is not None and price_per_share is not None
                                else None
                            )
                            underlying_security_value = (
                                underlying_security_share * price_per_share
                                if underlying_security_share is not None and price_per_share is not None
                                else None
                            )
                            holding_sk = (int(uuid.uuid4().int >> 64) % 9_990_000) + 10_000
                            record = (
                                "insider_trades.insider_holding_derivative_fact",
                                [
                                    "accession_number",
                                    "holding_sk",
                                    "security_title",
                                    "form_type",
                                    "conversion_price",
                                    "exercise_date",
                                    "expiration_date",
                                    "shares_owned_following",
                                    "value_owned_following",
                                    "ownership_type",
                                    "ownership_nature",
                                    "underlying_security_title",
                                    "underlying_security_shares",
                                    "underlying_security_value"
                                ],
                                [
                                    accession_number,
                                    holding_sk,
                                    security_title,
                                    form_type,
                                    conversion_price,
                                    exercise_date,
                                    expiration_date,
                                    shares_owned_following,
                                    value_owned_following,
                                    ownership_type,
                                    ownership_nature,
                                    underlying_security_title ,
                                    underlying_security_share,
                                    underlying_security_value
                                ]
                            )
                            state["holding_derivative"].append(record)
                        derivative_transaction_table = sb.get("derivativeTable",{})
                        derivative_transaction = []
                        if isinstance(derivative_transaction_table, dict):
                            derivative_transaction = derivative_transaction_table.get("derivativeTransaction", [])
                            if not isinstance(derivative_transaction, list):
                               derivative_transaction = [derivative_transaction]
                        for txn in derivative_transaction:
                            security_title = txn.get("securityTitle", {}).get("value", "")
                            shares_owned_data = txn.get("postTransactionAmounts", {}).get("sharesOwnedFollowingTransaction", {})
                            shares_owned_str = ""
                            if isinstance(shares_owned_data, dict):
                                shares_owned_str = shares_owned_data.get("value", None)
                            transaction_date = format_date(txn.get("transactionDate",{}).get("value",""))
                            deemed_execution_date_data =txn.get("deemedExecutionDate",{})
                            deemed_execution_date = None
                            if isinstance(deemed_execution_date_data, dict):
                                deemed_execution_date = deemed_execution_date_data.get("value",None)
                                deemed_execution_date = format_date(deemed_execution_date)
                            price_per_share_data = txn.get("transactionAmounts", {}).get("transactionPricePerShare", {})
                            price_per_share_str = ""
                            if isinstance(price_per_share_data, dict):
                                price_per_share_str = price_per_share_data.get("value", None)
                            form_type_data = txn.get("transactionCoding",{})
                            if isinstance(form_type_data, dict):
                                form_type = form_type_data.get("transactionFormType","")
                            transaction_code_data = txn.get("transactionCoding",{})
                            transaction_code = ""
                            if isinstance(transaction_code_data, dict):
                                transaction_code = transaction_code_data.get("transactionCode","")
                            equity_swap_involved_data = txn.get("transactionCoding", {})
                            if isinstance(equity_swap_involved_data, dict):
                                equity_swap_involved = equity_swap_involved_data.get("equitySwapInvolved") == "1"
                            transaction_timelines_data = txn.get("transactionTimelines",{})
                            transaction_timelines= ""
                            if isinstance(transaction_timelines_data, dict):
                                transaction_timelines = transaction_timelines_data.get("value","")
                            shares_data = txn.get("transactionAmounts", {}).get("transactionShares",{})
                            shares = None
                            if isinstance(shares_data, dict):
                                shares = shares_data.get("value", None)
                            transaction_type_data = txn.get("transactionAmounts",{}).get("transactionAcquiredDisposedCode",{})
                            transaction_type = ""
                            if isinstance(transaction_type_data, dict):
                                transaction_type = transaction_type_data.get("value","")
                            conversion_price_data = txn.get("conversionOrExercisePrice", {})
                            conversion_price=None
                            if isinstance(conversion_price_data, dict):
                                conversion_price= conversion_price_data.get("value", None)
                            exercise_date = format_date(txn.get("exerciseDate", {}).get("value") if isinstance(txn.get("exerciseDate"), dict) else None)
                            expiration_date = format_date(txn.get("expirationDate",{}).get("value") if isinstance(txn.get("expirationDate"), dict) else None)
                            ownership_type_data = txn.get("ownershipNature", {}).get("directOrIndirectOwnership", {})
                            ownership_type = ""
                            if isinstance(ownership_type_data, dict):
                                ownership_type = ownership_type_data.get("value", "")
                            ownership_nature_data = txn.get("ownershipNature", {}).get("natureOfOwnership", {})
                            ownership_nature = ""
                            if isinstance(ownership_nature_data, dict):
                                ownership_nature = ownership_nature_data.get("value", None)
                            underlying_security_title_data = txn.get("underlyingSecurity",{}).get("underlyingSecurityTitle",{})
                            underlying_security_title = ""
                            if isinstance(underlying_security_title_data, dict):
                                underlying_security_title = underlying_security_title_data.get("value","")
                            underlying_security_share_data = txn.get("underlyingSecurity",{}).get("underlyingSecurityShares",{})
                            underlying_security_share = ""
                            if isinstance(underlying_security_share_data, dict):
                                underlying_security_share = underlying_security_share_data.get("value",None)
                            try:
                                shares_owned_following = float(shares_owned_str) if shares_owned_str else None
                            except ValueError:
                                shares_owned_following = None
                            try:
                                price_per_share = float(price_per_share_str) if price_per_share_str else None
                            except ValueError:
                                price_per_share = None
                            try:
                                underlying_security_share = float(underlying_security_share) if underlying_security_share else None
                            except ValueError:
                                underlying_security_share = None
                            value_owned_following = (
                                shares_owned_following * price_per_share
                                if shares_owned_following is not None and price_per_share is not None
                                else None
                            )
                            underlying_security_value = (
                                underlying_security_share * price_per_share
                                if underlying_security_share is not None and price_per_share is not None
                                else None
                            )
                            transaction_sk = (int(uuid.uuid4().int >> 64) % 9_990_000) + 10_000
                            record = (
                                "insider_trades.insider_transaction_derivative_fact",
                                [
                                    "accession_number",
                                    "transaction_sk",
                                    "security_title",
                                    "transaction_date",
                                    "deemed_execution_date",
                                    "form_type",
                                    "transaction_code",
                                    "equity_swap_involved",
                                    "transaction_timeliness",
                                    "shares",
                                    "shares_owned_following",
                                    "value_owned_following",
                                    "price_per_share",
                                    "conversion_price",
                                    "transaction_type",
                                    "ownership_type",
                                    "ownership_nature",
                                    "underlying_security_title",
                                    "underlying_security_shares",
                                    "underlying_security_value",
                                    "exercise_date",
                                    "expiration_date",
                                
                                ],
                                [
                                    accession_number,
                                    transaction_sk,
                                    security_title,
                                    transaction_date,
                                    deemed_execution_date,
                                    form_type,
                                    transaction_code,
                                    equity_swap_involved,
                                    transaction_timelines,
                                    shares,
                                    shares_owned_following,
                                    value_owned_following,
                                    price_per_share,
                                    conversion_price,
                                    transaction_type,
                                    ownership_type,
                                    ownership_nature,
                                    underlying_security_title ,
                                    underlying_security_share,
                                    underlying_security_value,
                                    exercise_date,
                                    expiration_date

                                ]
                            )
                            state["transaction_derivative"].append(record)
                        non_derivative_transaction_table = sb.get("nonDerivativeTable",{})
                        non_derivative_transaction =[]
                        if isinstance(non_derivative_transaction_table, dict):
                            non_derivative_transaction = non_derivative_transaction_table.get("nonDerivativeTransaction",[])
                            if not isinstance(non_derivative_transaction, list):
                               non_derivative_transaction = [non_derivative_transaction]
                        for txn in non_derivative_transaction:
                            # Extract field values
                            security_title = txn.get("securityTitle", {}).get("value", "")
                            shares_owned_data = txn.get("postTransactionAmounts", {}).get("sharesOwnedFollowingTransaction", {})
                            shares_owned_str =""
                            if isinstance(shares_owned_data, dict):
                                shares_owned_str = shares_owned_data.get("value", None)
                            transaction_date = format_date(txn.get("transactionDate",{}).get("value",""))
                            deemed_execution_date_data =txn.get("deemedExecutionDate",{})
                            deemed_execution_date = None
                            if isinstance(deemed_execution_date_data, dict):
                                deemed_execution_date = format_date(deemed_execution_date_data.get("value",""))
                            price_per_share_data = txn.get("transactionAmounts", {}).get("transactionPricePerShare", {})
                            price_per_share_str = ""
                            if isinstance(price_per_share_data, dict):
                                price_per_share_str = price_per_share_data.get("value", None)
                            form_type_data = txn.get("transactionCoding",{})
                            form_type = ""
                            if isinstance(form_type_data, dict):
                                form_type = form_type_data.get("transactionFormType","")
                            transaction_code_data = txn.get("transactionCoding",{})
                            transaction_code = ""
                            if isinstance(transaction_code_data, dict):
                                transaction_code = transaction_code_data.get("transactionCode","")
                            equity_swap_involved_data = txn.get("transactionCoding", {})
                            equity_swap_involved = None
                            if isinstance(equity_swap_involved_data, dict):
                                equity_swap_involved = equity_swap_involved_data.get("equitySwapInvolved") == "1"
                            transaction_timelines_data = txn.get("transactionTimelines",{})
                            transaction_timelines = ""
                            if isinstance(transaction_timelines_data, dict):
                                transaction_timelines = transaction_timelines_data.get("value","")
                            shares_data = txn.get("transactionAmounts", {}).get("transactionShares",{})
                            shares = None
                            if isinstance(shares_data, dict):
                                shares = shares_data.get("value", None)
                            transaction_type_data = txn.get("transactionAmounts",{}).get("transactionAcquiredDisposedCode",{})
                            transaction_type = ""
                            if isinstance(transaction_type_data, dict):
                                transaction_type = transaction_type_data.get("value","")
                            ownership_type_data = txn.get("ownershipNature", {}).get("directOrIndirectOwnership", {})
                            ownership_type = ""
                            if isinstance(ownership_type_data, dict):
                                ownership_type = ownership_type_data.get("value", "")
                            ownership_nature_data = txn.get("ownershipNature", {}).get("natureOfOwnership", {})
                            ownership_nature = ""
                            if isinstance(ownership_nature_data, dict):
                                ownership_nature = ownership_nature_data.get("value", None)
                            try:
                                shares_owned_following = float(shares_owned_str) if shares_owned_str else None
                            except ValueError:
                                shares_owned_following = None
                            try:
                                price_per_share = float(price_per_share_str) if price_per_share_str else None
                            except ValueError:
                                price_per_share = None
                        
                            value_owned_following = (
                                shares_owned_following * price_per_share
                                if shares_owned_following is not None and price_per_share is not None
                                else None
                            )
                            transaction_sk = (int(uuid.uuid4().int >> 64) % 9_990_000) + 10_000
                            record = (
                                "insider_trades.insider_transaction_non_derivative_fact",
                                [
                                    "accession_number",
                                    "transaction_sk",
                                    "security_title",
                                    "transaction_date",
                                    "deemed_execution_date",
                                    "form_type",
                                    "transaction_code",
                                    "equity_swap_involved",
                                    "transaction_timeliness",
                                    "shares",
                                    "shares_owned_following",
                                    "value_owned_following",
                                    "price_per_share",
                                    "transaction_type",
                                    "ownership_type",
                                    "ownership_nature",
                                ],
                                [
                                    accession_number,
                                    transaction_sk,
                                    security_title,
                                    transaction_date,
                                    deemed_execution_date,
                                    form_type,
                                    transaction_code,
                                    equity_swap_involved,
                                    transaction_timelines,
                                    shares,
                                    shares_owned_following,
                                    value_owned_following,
                                    price_per_share,
                                    transaction_type,
                                    ownership_type,
                                    ownership_nature,
                                ]
                            )
                            state["transaction_non_derivative"].append(record)
                        footnotes = sb.get("footnotes")
                        if isinstance(footnotes, dict):
                            footnote = footnotes.get("footnote", [])
                            if isinstance(footnote, dict):
                                footnote = [footnote]
                            for fn in footnote:
                                footnote_id = fn.get("@id", "")
                                footnote_text = fn.get("#text", "").strip() if isinstance(fn, dict) else None
                                record = (
                                    "insider_trades.insider_footnote_dimension",
                                    ["accession_number", "footnote_id", "footnote_text"],
                                    [accession_number, footnote_id, footnote_text]
                                )
                                state["footnote_dimension"].append(record)   
                        else:
                            print(f"⚠️ No footnotes found in accession {accession_number}")
                   
                if state["insider_submission_dimension"]: insert_one(cur, *state["insider_submission_dimension"])
                if state["insider_owner_signature_fact"]: insert_one(cur, *state["insider_owner_signature_fact"])
                reporting_owner_records = state["reporting_owner"]
                if reporting_owner_records:
                    table_name, columns, _ = reporting_owner_records[0]
                    values = [row[2] for row in reporting_owner_records]  # only the value list
                    insert_many(cur, table_name, columns, values)
                holding_non_derivative_records = state["holding_non_derivative"]
                if holding_non_derivative_records:
                    table_name, columns, _ = holding_non_derivative_records[0]
                    values = [row[2] for row in holding_non_derivative_records]
                    insert_many(cur, table_name, columns, values)

                holding_derivative_records = state["holding_derivative"]
                if holding_derivative_records:
                    table_name, columns, _ = holding_derivative_records[0]
                    values = [row[2] for row in holding_derivative_records]
                    insert_many(cur, table_name, columns, values)

                transaction_derivative_records = state["transaction_derivative"]
                if transaction_derivative_records:
                    table_name, columns, _ = transaction_derivative_records[0]
                    values = [row[2] for row in transaction_derivative_records]
                    insert_many(cur, table_name, columns, values)

                transaction_non_derivative_records = state["transaction_non_derivative"]
                if transaction_non_derivative_records:
                    table_name, columns, _ = transaction_non_derivative_records[0]
                    values = [row[2] for row in transaction_non_derivative_records]
                    insert_many(cur, table_name, columns, values)

                footnote_dimension_records = state["footnote_dimension"]
                if footnote_dimension_records:
                    table_name, columns, _ = footnote_dimension_records[0]
                    values = [row[2] for row in footnote_dimension_records]
                    insert_many(cur, table_name, columns, values)


                cur.execute("UPDATE pipelines.form_345_jobs SET status='SUCCESS', last_attempted_at=NOW() WHERE accession_number=%s", [accession_number])

                print(f"✅ Job {accession_number} marked SUCCESS")
            except Exception as e:
                print(f"❌ Error processing {accession_number}: {e}")
                traceback.print_exc()
                cur.execute("UPDATE pipelines.form_345_jobs SET status='FAILED', error_message=%s, last_attempted_at=NOW() WHERE accession_number=%s", [str(e), accession_number])
if __name__ == "__main__":
    main()