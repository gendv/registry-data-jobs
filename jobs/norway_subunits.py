"""Brreg whole sub-unit register (underenheter, bulk download) -> 'Norway sub-units (auto)'."""
import gzip

import ijson
import requests

from common import guarded, log, run_job

URL = "https://data.brreg.no/enhetsregisteret/api/underenheter/lastned"
LOCAL = "/tmp/underenheter.json.gz"


def download():
    log(f"downloading {URL}")
    with requests.get(URL, stream=True, timeout=900, headers={"Accept": "application/vnd.brreg.enhetsregisteret.underenhet.v2+gzip;charset=UTF-8"}) as r:
        r.raise_for_status()
        version = r.headers.get("Last-Modified") or r.headers.get("ETag") or ""
        with open(LOCAL, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    return version


def records():
    with gzip.open(LOCAL, "rb") as f:
        for e in ijson.items(f, "item", use_float=True):
            num = str(e.get("organisasjonsnummer") or "").strip()
            if num:
                yield num, e.get("registreringsdatoEnhetsregisteret"), e


def main():
    version = download()
    run_job("Norway sub-units (auto)", records, source_id=version)


if __name__ == "__main__":
    guarded("Norway sub-units (auto)", main)
