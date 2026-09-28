#!/usr/bin/env python3
"""Executes upgrade-app.yml's "Download Spring Application Advisor CLI" step — the bytes that
ship, extracted from the workflow — against a local HTTP server, and pins the step's placement.

Why (boostertickets #649, 2026-09-28): the download used to run inside the advisor step, which
is continue-on-error. `curl -L` without -f wrote a 401 body where the tarball belonged, tar
failed, nothing wrote .advisor/errors/, and the run went GREEN having done nothing. The Broadcom
token lasts 2 days, so this is the common failure, not a corner case.
"""
import http.server
import io
import os
import pathlib
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "upgrade-app.yml"
DOWNLOAD = "Download Spring Application Advisor CLI"
ADVISOR = "Run Spring Application Advisor"

failures = []


def fail(msg):
    failures.append(msg)
    print(f"FAIL: {msg}")


def tar_bytes(members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as t:
        for name, body, mode in members:
            info = tarfile.TarInfo(name)
            info.size = len(body)
            info.mode = mode
            t.addfile(info, io.BytesIO(body))
    return buf.getvalue()


ROUTES = {
    "/expired": (401, b'{"errors":[{"status":401,"message":"Token failed verification: expired"}]}'),
    "/junk": (200, b"<html>not a tarball</html>"),
    "/noadv.tar": (200, tar_bytes([("pkg/README", b"hi", 0o644)])),
    "/good.tar": (200, tar_bytes([("pkg/advisor", b"#!/bin/sh\necho advisor\n", 0o755)])),
}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        code, body = ROUTES.get(self.path, (404, b"{}"))
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def steps():
    wf = yaml.safe_load(WORKFLOW.read_text())
    return [s for job in wf["jobs"].values() for s in job.get("steps", [])]


def structure(all_steps):
    names = [s.get("name") for s in all_steps]
    dl = [s for s in all_steps if s.get("name") == DOWNLOAD]
    adv = [s for s in all_steps if s.get("name") == ADVISOR]
    if len(dl) != 1 or len(adv) != 1:
        fail(f"expected one '{DOWNLOAD}' and one '{ADVISOR}' step, found {len(dl)} and {len(adv)}")
        return None
    dl, adv = dl[0], adv[0]
    if "continue-on-error" in dl:
        fail("the download step carries continue-on-error — a failed download would read green again")
    if "if" in dl:
        fail("the download step carries an if: — a skipped download must not be possible")
    if names.index(DOWNLOAD) > names.index(ADVISOR):
        fail("the download step runs after the advisor step")
    for leaked in ("CLI_DOWNLOAD_URL", "curl "):
        if leaked in adv.get("run", ""):
            fail(f"the advisor step (continue-on-error) mentions {leaked!r} — a download inside it is masked")
    return dl["run"]


def run_step(body, env_extra):
    with tempfile.TemporaryDirectory() as scratch:
        genv = pathlib.Path(scratch, "github_env")
        genv.write_text("")
        env = {"PATH": os.environ["PATH"], "HOME": scratch, "GITHUB_ENV": str(genv), **env_extra}
        # GitHub runs `run:` bodies with `bash -e {0}` when no shell is set.
        p = subprocess.run(["bash", "-e", "-c", body], env=env, capture_output=True, text=True, timeout=120)
        exported = dict(
            line.split("=", 1) for line in genv.read_text().splitlines() if "=" in line
        )
        advisor_ok = False
        tmp = exported.get("ADVISOR_TMP_DIR")
        if tmp:
            adv = pathlib.Path(tmp, "advisor")
            advisor_ok = adv.is_file() and bool(adv.stat().st_mode & stat.S_IXUSR)
            subprocess.run(["rm", "-rf", tmp], check=False)
        return p.returncode, p.stdout + p.stderr, advisor_ok


def main():
    body = structure(steps())
    if body is None:
        return 1
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    cases = [
        # name, url, token, want_rc_zero, must_contain, want_advisor
        ("empty url", "", "t", False, "SPRING_APP_ADVISOR_DOWNLOAD_URL is empty", False),
        ("empty token", f"{base}/good.tar", "", False, "BROADCOM_SPRING_PASSWORD is empty", False),
        ("expired token (401)", f"{base}/expired", "t", False, "HTTP 401", False),
        ("error body served as 200", f"{base}/junk", "t", False, "", False),
        ("archive without advisor", f"{base}/noadv.tar", "t", False, "no executable 'advisor'", False),
        ("good archive", f"{base}/good.tar", "t", True, "SAA CLI ready: HTTP 200", True),
    ]
    try:
        for name, url, token, ok, needle, want_adv in cases:
            rc, out, adv = run_step(body, {"CLI_DOWNLOAD_URL": url, "ARTIFACTORY_TOKEN": token})
            if (rc == 0) != ok:
                fail(f"[{name}] exit {rc}, want {'0' if ok else 'non-zero'}\n{out[-400:]}")
            elif needle and needle not in out:
                fail(f"[{name}] output lacks {needle!r}\n{out[-400:]}")
            elif adv != want_adv:
                fail(f"[{name}] exported advisor executable={adv}, want {want_adv}")
            else:
                print(f"ok: {name} (exit {rc})")
    finally:
        server.shutdown()
    if failures:
        print(f"{len(failures)} failure(s)")
        return 1
    print(f"test_cli_download: OK — {len(cases)} executed cases + step placement")
    return 0


if __name__ == "__main__":
    sys.exit(main())
