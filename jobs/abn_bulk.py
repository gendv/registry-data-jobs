"""ABN Lookup bulk extract (data.gov.au) -> 'Australia businesses ABN (auto)'.

Keeps active ABNs that are not individuals (sole traders are never emailed).
Each <ABR> element is converted to a dict mechanically.
"""
import os
import zipfile

import requests
from lxml import etree

from common import clean, guarded, log, run_job

PACKAGE = "https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract"
KEEP_INDIVIDUALS = os.environ.get("KEEP_INDIVIDUALS") == "1"


def zip_resources():
    res = requests.get(PACKAGE, timeout=60).json()["result"]["resources"]
    # keep the API's order: continuation skip counts depend on it
    return [r for r in res if (r.get("format") or "").upper() == "ZIP"]


def download(url, path):
    log(f"downloading {url}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


def to_dict(el):
    out = dict(el.attrib)
    children = list(el)
    if not children:
        text = (el.text or "").strip()
        if out:
            if text:
                out["text"] = text
            return out
        return text
    for c in children:
        v = to_dict(c)
        if c.tag in out:
            if not isinstance(out[c.tag], list):
                out[c.tag] = [out[c.tag]]
            out[c.tag].append(v)
        else:
            out[c.tag] = v
    return out


def yyyymmdd(v):
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}" if v and len(v) == 8 and v.isdigit() else None


def records(paths):
    kept = skipped = 0
    for path in paths:
        with zipfile.ZipFile(path) as z:
            for name in sorted(z.namelist()):
                if not name.lower().endswith(".xml"):
                    continue
                log(f"reading {name}")
                with z.open(name) as f:
                    for _, el in etree.iterparse(f, tag="ABR"):
                        abn_el = el.find("ABN")
                        etype = el.findtext("EntityType/EntityTypeInd")
                        status = abn_el.get("status") if abn_el is not None else None
                        if status != "ACT" or (etype == "IND" and not KEEP_INDIVIDUALS):
                            skipped += 1
                        else:
                            item = clean(to_dict(el))
                            abn = (abn_el.text or "").strip()
                            if abn:
                                kept += 1
                                yield abn, yyyymmdd(abn_el.get("ABNStatusFromDate")), item
                        el.clear()
                        while el.getprevious() is not None:
                            del el.getparent()[0]
    log(f"kept {kept:,}, skipped {skipped:,} (cancelled or individuals)")


def main():
    paths, ids = [], []
    for i, res in enumerate(zip_resources()):
        p = f"/tmp/abn_{i}.zip"
        download(res["url"], p)
        paths.append(p)
        ids.append((res.get("last_modified") or res.get("metadata_modified") or res["url"])[:19])
    run_job("Australia businesses ABN (auto)", lambda: records(paths), source_id="|".join(ids))


if __name__ == "__main__":
    guarded("Australia businesses ABN (auto)", main)
