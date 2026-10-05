"""Florida Sunbiz quarterly corporate file -> 'Florida companies (auto)'.

Downloads the newest cordata*.zip from the state's public SFTP server and
parses the fixed-width 1440-character records (same layout as the daily files
handled in n8n).
"""
import io
import os
import stat
import zipfile

import paramiko

from common import clean, guarded, log, run_job, valid_day

HOST = "sftp.floridados.gov"
FOLDER = "/Public/doc/Quarterly/Cor"
LOCAL = "/tmp/cordata.zip"


def _connect():
    transport = paramiko.Transport((HOST, 22))
    transport.banner_timeout = 60
    transport.connect(username=os.environ["FL_SFTP_USER"], password=os.environ["FL_SFTP_PASSWORD"])
    transport.set_keepalive(15)
    return transport, paramiko.SFTPClient.from_transport(transport)


def download():
    """Resumable download: the state server drops long transfers, so read in blocks
    and reconnect from the last offset when the connection goes."""
    import time
    transport, sftp = _connect()
    files = [f for f in sftp.listdir_attr(FOLDER)
             if not stat.S_ISDIR(f.st_mode) and f.filename.lower().startswith("cordata") and f.filename.lower().endswith(".zip")]
    newest = max(files, key=lambda f: f.st_mtime)
    size, mtime = newest.st_size, newest.st_mtime
    path = f"{FOLDER}/{newest.filename}"
    log(f"downloading {newest.filename} ({size / 1e9:.2f} GB)")
    done, failures, last_log = 0, 0, 0
    with open(LOCAL, "wb") as out:
        while done < size:
            try:
                with sftp.open(path, "rb") as remote:
                    remote.seek(done)
                    remote.prefetch(size - done)  # pipeline reads instead of one 32 KB round trip at a time
                    while done < size:
                        chunk = remote.read(1 << 20)
                        if not chunk:
                            raise EOFError("empty read before end of file")
                        out.write(chunk)
                        done += len(chunk)
                        if done - last_log >= 200 << 20:
                            log(f"{done / 1e9:.2f} GB")
                            last_log = done
            except (paramiko.SSHException, paramiko.SFTPError, OSError, EOFError) as e:
                failures += 1
                log(f"connection dropped at {done / 1e9:.2f} GB ({e}); reconnect {failures}")
                if failures > 50:
                    raise
                try:
                    transport.close()
                except Exception:
                    pass
                time.sleep(min(60, 5 * failures))
                transport, sftp = _connect()
                st = sftp.stat(path)
                if st.st_size != size or st.st_mtime != mtime:
                    raise RuntimeError("the file changed on the server during download")
    sftp.close()
    transport.close()
    return newest.filename, f"{newest.filename}:{int(mtime)}:{size}"


def s(line, a, n):
    return line[a - 1:a - 1 + n].replace("\x00", "").strip()


def d(v):
    return valid_day(f"{v[4:]}-{v[0:2]}-{v[2:4]}") if len(v) == 8 and v.isdigit() else None


def person(line, a, typ):
    if typ == "P":
        return clean({"last": s(line, a, 20), "first": s(line, a + 20, 14), "middle": s(line, a + 34, 8)})
    return clean({"name": s(line, a, 42)})


def parse(line):
    reports = []
    for k in range(3):
        b = 506 + 13 * k
        r = clean({"year": s(line, b, 4), "house_flag": s(line, b + 4, 1), "date": d(s(line, b + 5, 8))})
        if r:
            reports.append(r)
    officers = []
    for k in range(6):
        b = 669 + 128 * k
        typ = s(line, b + 4, 1)
        o = clean({"title": s(line, b, 4), "type": typ, "name": person(line, b + 5, typ),
                   "address": s(line, b + 47, 42), "city": s(line, b + 89, 28),
                   "state": s(line, b + 117, 2), "zip": s(line, b + 119, 9)})
        if o.get("name") or o.get("title"):
            officers.append(o)
    ra_type = s(line, 587, 1)
    return clean({
        "doc_number": s(line, 1, 12), "name": s(line, 13, 192), "status": s(line, 205, 1), "filing_type": s(line, 206, 15),
        "principal": {"address1": s(line, 221, 42), "address2": s(line, 263, 42), "city": s(line, 305, 28),
                      "state": s(line, 333, 2), "zip": s(line, 335, 10), "country": s(line, 345, 2)},
        "mailing": {"address1": s(line, 347, 42), "address2": s(line, 389, 42), "city": s(line, 431, 28),
                    "state": s(line, 459, 2), "zip": s(line, 461, 10), "country": s(line, 471, 2)},
        "file_date": d(s(line, 473, 8)), "fei_number": s(line, 481, 14), "more_than_six_officers": s(line, 495, 1),
        "last_transaction_date": d(s(line, 496, 8)), "state_country": s(line, 504, 2),
        "annual_reports": reports,
        "registered_agent": {"type": ra_type, "name": person(line, 545, ra_type), "address": s(line, 588, 42),
                             "city": s(line, 630, 28), "state": s(line, 658, 2), "zip": s(line, 660, 9)},
        "officers": officers,
    })


def records():
    with zipfile.ZipFile(LOCAL) as z:
        for name in sorted(z.namelist()):
            if not name.lower().endswith(".txt"):
                continue
            log(f"reading {name}")
            with z.open(name) as raw:
                for line in io.TextIOWrapper(raw, encoding="latin-1", newline=None):
                    line = line.rstrip("\r\n")
                    if len(line) < 1000:
                        continue
                    item = parse(line)
                    if item.get("doc_number"):
                        yield item["doc_number"], item.get("file_date"), item


def main():
    fname, version = download()
    run_job("Florida companies (auto)", records, source_id=version)
    log(f"finished {fname}")


if __name__ == "__main__":
    guarded("Florida companies (auto)", main)
