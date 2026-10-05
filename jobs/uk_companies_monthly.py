"""Companies House 'Free Company Data Product' monthly snapshot -> 'UK companies (auto)'.

One CSV with every live company on the register (name, number, address,
category, status, incorporation date, SIC codes, previous names).
"""
import csv
import io
import re
import zipfile

import requests

from common import clean, guarded, log, run_job, valid_day

PAGE = "https://download.companieshouse.gov.uk/en_output.html"
LOCAL = "/tmp/uk_basic.zip"


def latest_url():
    html = requests.get(PAGE, timeout=60).text
    names = sorted(set(re.findall(r"BasicCompanyDataAsOneFile-\d{4}-\d{2}-\d{2}\.zip", html)))
    if not names:
        raise RuntimeError("could not find the monthly file on the download page")
    return "https://download.companieshouse.gov.uk/" + names[-1]


def download(url):
    log(f"downloading {url}")
    with requests.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(LOCAL, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


def dmy(v):
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", (v or "").strip())
    return valid_day(f"{m.group(3)}-{m.group(2)}-{m.group(1)}") if m else None


def records():
    with zipfile.ZipFile(LOCAL) as z:
        name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        log(f"reading {name}")
        with z.open(name) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline=""))
            header = [h.strip() for h in next(reader)]
            for row in reader:
                rec = {header[i]: row[i] for i in range(min(len(header), len(row)))}
                num = (rec.get("CompanyNumber") or "").strip()
                if not num:
                    continue
                sic = [rec.get(f"SICCode.SicText_{i}") for i in range(1, 5)]
                prev = []
                for i in range(1, 11):
                    n = rec.get(f"PreviousName_{i}.CompanyName")
                    if n and n.strip():
                        prev.append({"name": n, "changed_on": dmy(rec.get(f"PreviousName_{i}.CONDATE"))})
                item = clean({
                    "company_number": num,
                    "name": rec.get("CompanyName"),
                    "category": rec.get("CompanyCategory"),
                    "status": rec.get("CompanyStatus"),
                    "country_of_origin": rec.get("CountryOfOrigin"),
                    "incorporated_on": dmy(rec.get("IncorporationDate")),
                    "dissolved_on": dmy(rec.get("DissolutionDate")),
                    "registered_address": {
                        "care_of": rec.get("RegAddress.CareOf"), "po_box": rec.get("RegAddress.POBox"),
                        "line1": rec.get("RegAddress.AddressLine1"), "line2": rec.get("RegAddress.AddressLine2"),
                        "town": rec.get("RegAddress.PostTown"), "county": rec.get("RegAddress.County"),
                        "country": rec.get("RegAddress.Country"), "postcode": rec.get("RegAddress.PostCode"),
                    },
                    "sic": [x for x in sic if x and x.strip() and x.strip() != "None Supplied"],
                    "accounts": {"category": rec.get("Accounts.AccountCategory"),
                                 "next_due": dmy(rec.get("Accounts.NextDueDate")),
                                 "last_made_up": dmy(rec.get("Accounts.LastMadeUpDate"))},
                    "confirmation_statement": {"next_due": dmy(rec.get("ConfStmtNextDueDate")),
                                               "last_made_up": dmy(rec.get("ConfStmtLastMadeUpDate"))},
                    "previous_names": prev,
                    "uri": rec.get("URI"),
                })
                yield num, item.get("incorporated_on"), item


def main():
    url = latest_url()
    download(url)
    run_job("UK companies (auto)", records, source_id=url.rsplit("/", 1)[-1])


if __name__ == "__main__":
    guarded("UK companies (auto)", main)
