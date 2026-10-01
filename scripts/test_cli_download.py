#!/usr/bin/env python3
"""Executes each reusable workflow's "Download Spring Application Advisor CLI" step — the bytes
that ship, extracted from the workflow — against a local HTTP server, and pins the step's placement.

Why (boostertickets #649, 2026-09-28): the download used to run inside the advisor step, which
is continue-on-error. `curl -L` without -f wrote a 401 body where the tarball belonged, tar
failed, nothing wrote .advisor/errors/, and the run went GREEN having done nothing. The Broadcom
token lasts 2 days, so this is the common failure, not a corner case. upgrade-app.yml was fixed
in #4; the mapping workflows (#5) carry the same step plus a self-hosted preinstalled-CLI path.

Every copy is executed — there is one per workflow, and a test restating one would be a fourth.
The mapping workflows' masked step is executed too, against a fake `advisor`, to prove it runs
the CLI the download step exported.
"""
import http.server
import io
import os
import pathlib
import re
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
DOWNLOAD = "Download Spring Application Advisor CLI"
# workflow file -> the continue-on-error step that consumes the CLI
CONSUMERS = {
    "upgrade-app.yml": "Run Spring Application Advisor",
    "build-mapping.yml": "Build mapping",
    "create-mapping.yml": "Create mapping",
}
# Workflows whose runs-on: self-hosted uses the runner's preinstalled advisor instead of downloading.
SELF_HOSTED_PREINSTALLED = {"build-mapping.yml", "create-mapping.yml"}
# Mapping consumers executed against a fake CLI: the advisor argv they must produce.
CONSUMER_RUNS = {
    "build-mapping.yml": "mapping build -r https://example.invalid/repo -c g:a -s slug",
    "create-mapping.yml": "mapping create -c g:a",
}

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


def load(fname):
    wf = yaml.safe_load((WORKFLOWS / fname).read_text())
    on = wf.get("on", wf.get(True))  # PyYAML reads a bare `on:` key as True
    inputs = (on.get("workflow_call") or {}).get("inputs") or {}
    all_steps = [s for job in wf["jobs"].values() for s in job.get("steps", [])]
    return inputs, all_steps


def structure(fname, all_steps):
    consumer = CONSUMERS[fname]
    names = [s.get("name") for s in all_steps]
    dl = [s for s in all_steps if s.get("name") == DOWNLOAD]
    adv = [s for s in all_steps if s.get("name") == consumer]
    if len(dl) != 1 or len(adv) != 1:
        fail(f"{fname}: expected one '{DOWNLOAD}' and one '{consumer}' step, found {len(dl)} and {len(adv)}")
        return None, None
    dl, adv = dl[0], adv[0]
    if "continue-on-error" in dl:
        fail(f"{fname}: the download step carries continue-on-error — a failed download would read green again")
    if "if" in dl:
        fail(f"{fname}: the download step carries an if: — a skipped download must not be possible")
    if names.index(DOWNLOAD) > names.index(consumer):
        fail(f"{fname}: the download step runs after '{consumer}'")
    for leaked in ("CLI_DOWNLOAD_URL", "ARTIFACTORY_TOKEN", "curl ", "advisor-linux"):
        if leaked in adv.get("run", "") or leaked in (adv.get("env") or {}):
            fail(f"{fname}: '{consumer}' (continue-on-error) mentions {leaked!r} — a download inside it is masked")
    return dl, adv


def render(body):
    """Stand in for the platform's ${{ }} expansion: every expression renders empty (inputs unset)."""
    return re.sub(r"\$\{\{.*?\}\}", "", body)


def bash(body, env_extra, cwd=None):
    with tempfile.TemporaryDirectory() as scratch:
        genv = pathlib.Path(scratch, "github_env")
        genv.write_text("")
        env = {"PATH": os.environ["PATH"], "HOME": scratch, "GITHUB_ENV": str(genv), **env_extra}
        # GitHub runs `run:` bodies with `bash -e {0}` when no shell is set.
        p = subprocess.run(["bash", "-e", "-c", render(body)], env=env, cwd=cwd or scratch,
                           capture_output=True, text=True, timeout=120)
        exported = dict(line.split("=", 1) for line in genv.read_text().splitlines() if "=" in line)
        return p.returncode, p.stdout + p.stderr, exported


def exported_cli(exported):
    """The CLI path the step handed to later steps: ADVISOR_CLI, or upgrade-app's ADVISOR_TMP_DIR/advisor."""
    if "ADVISOR_CLI" in exported:
        return exported["ADVISOR_CLI"]
    if "ADVISOR_TMP_DIR" in exported:
        return str(pathlib.Path(exported["ADVISOR_TMP_DIR"], "advisor"))
    return None


def is_exe(path):
    p = pathlib.Path(path) if path else None
    return bool(p and p.is_file() and p.stat().st_mode & stat.S_IXUSR)


def path_without_advisor():
    return os.pathsep.join(d for d in os.environ["PATH"].split(os.pathsep)
                           if d and not pathlib.Path(d, "advisor").exists())


def fake_advisor(directory, record):
    """An `advisor` that records its argv and writes a mapping where `mapping create` leaves one."""
    exe = pathlib.Path(directory, "advisor")
    exe.write_text(f'#!/bin/sh\necho "$*" > "{record}"\nmkdir -p .advisor/mappings && echo {{}} > .advisor/mappings/x.json\n')
    exe.chmod(0o755)
    return exe


def run_download(fname, body, base, runs_on_default):
    runs_on = {"RUNS_ON": runs_on_default} if runs_on_default is not None else {}
    cases = [
        # name, url, token, want_rc_zero, must_contain, want_advisor
        ("empty url", "", "t", False, "SPRING_APP_ADVISOR_DOWNLOAD_URL is empty", False),
        ("empty token", f"{base}/good.tar", "", False, "BROADCOM_SPRING_PASSWORD is empty", False),
        ("expired token (401)", f"{base}/expired", "t", False, "HTTP 401", False),
        ("error body served as 200", f"{base}/junk", "t", False, "", False),
        ("archive without advisor", f"{base}/noadv.tar", "t", False, "no executable 'advisor'", False),
        ("good archive", f"{base}/good.tar", "t", True, "SAA CLI ready: HTTP 200", True),
    ]
    for name, url, token, ok, needle, want_adv in cases:
        label = f"{fname} [{name}{', runs-on=' + runs_on_default if runs_on else ''}]"
        rc, out, exported = bash(body, {"CLI_DOWNLOAD_URL": url, "ARTIFACTORY_TOKEN": token, **runs_on})
        adv = is_exe(exported_cli(exported))
        if exported.get("ADVISOR_TMP_DIR"):
            subprocess.run(["rm", "-rf", exported["ADVISOR_TMP_DIR"]], check=False)
        if (rc == 0) != ok:
            fail(f"{label} exit {rc}, want {'0' if ok else 'non-zero'}\n{out[-400:]}")
        elif needle and needle not in out:
            fail(f"{label} output lacks {needle!r}\n{out[-400:]}")
        elif adv != want_adv:
            fail(f"{label} exported advisor executable={adv}, want {want_adv}")
        else:
            print(f"ok: {label} (exit {rc})")
    return len(cases)


def run_self_hosted(fname, body):
    """runs-on: self-hosted uses the runner's preinstalled advisor — and never downloads."""
    n = 0
    with tempfile.TemporaryDirectory() as bindir:
        fake = fake_advisor(bindir, os.devnull)
        env = {"RUNS_ON": "self-hosted", "CLI_DOWNLOAD_URL": "", "ARTIFACTORY_TOKEN": "",
               "PATH": bindir + os.pathsep + path_without_advisor()}
        rc, out, exported = bash(body, env)
        n += 1
        if rc != 0 or exported.get("ADVISOR_CLI") != str(fake):
            fail(f"{fname} [self-hosted, advisor on PATH] exit {rc}, ADVISOR_CLI={exported.get('ADVISOR_CLI')!r}, want {str(fake)!r}\n{out[-400:]}")
        else:
            print(f"ok: {fname} [self-hosted, advisor on PATH] (exit 0, no download)")
    rc, out, exported = bash(body, {"RUNS_ON": "self-hosted", "CLI_DOWNLOAD_URL": "", "ARTIFACTORY_TOKEN": "",
                                    "PATH": path_without_advisor()})
    n += 1
    if rc == 0 or "no 'advisor' is on PATH" not in out or exported_cli(exported):
        fail(f"{fname} [self-hosted, no advisor on PATH] exit {rc}, want non-zero naming the missing CLI\n{out[-400:]}")
    else:
        print(f"ok: {fname} [self-hosted, no advisor on PATH] (exit {rc})")
    return n


def run_consumer(fname, adv):
    """The masked step runs the CLI the download step exported — and refuses when none was."""
    want = CONSUMER_RUNS[fname]
    env = {"MAPPING_CONFIG_REPO": "https://example.invalid/repo", "MAPPING_CONFIG_COORDINATES": "g:a",
           "MAPPING_CONFIG_SLUG": "slug", "PATH": path_without_advisor()}
    with tempfile.TemporaryDirectory() as work, tempfile.TemporaryDirectory() as bindir:
        record = pathlib.Path(bindir, "argv")
        fake = fake_advisor(bindir, record)
        for d in ("ignore", ".advisor/mappings"):
            pathlib.Path(work, d).mkdir(parents=True)
        rc, out, _ = bash(adv["run"], {**env, "ADVISOR_CLI": str(fake)}, cwd=work)
        got = record.read_text().strip() if record.exists() else None
        if rc != 0 or got != want:
            fail(f"{fname} [consumer, exported CLI] exit {rc}, advisor argv {got!r}, want {want!r}\n{out[-400:]}")
        else:
            print(f"ok: {fname} [consumer runs the exported CLI: advisor {got}]")
        rc, out, _ = bash(adv["run"], env, cwd=work)
        if rc == 0 or "no advisor CLI" not in out:
            fail(f"{fname} [consumer, nothing exported] exit {rc}, want non-zero naming the missing CLI\n{out[-400:]}")
        else:
            print(f"ok: {fname} [consumer refuses with nothing exported] (exit {rc})")
    return 2


def main():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    executed = 0
    try:
        for fname in CONSUMERS:
            inputs, all_steps = load(fname)
            dl, adv = structure(fname, all_steps)
            if dl is None:
                continue
            # Real callers take the input's default (all saa-mappings callers of create-mapping
            # pass no runs-on, so they run on 'java' and download) — execute that branch.
            runs_on = inputs.get("runs-on")
            default = runs_on.get("default") if runs_on and fname in SELF_HOSTED_PREINSTALLED else None
            executed += run_download(fname, dl["run"], base, default)
            if fname in SELF_HOSTED_PREINSTALLED:
                executed += run_self_hosted(fname, dl["run"])
            if fname in CONSUMER_RUNS:
                executed += run_consumer(fname, adv)
    finally:
        server.shutdown()
    if failures:
        print(f"{len(failures)} failure(s)")
        return 1
    print(f"test_cli_download: OK — {executed} executed cases + step placement across {len(CONSUMERS)} workflows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
