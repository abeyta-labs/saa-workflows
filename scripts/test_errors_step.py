#!/usr/bin/env python3
"""Pins each reusable workflow's "Get errors if exist" step — the backstop that turns advisor
errors into a red run, since the advisor step itself is continue-on-error.

Why: hashFiles() takes FILE globs, and a bare directory path ('.advisor/errors/') matches no
file, so the step never ran and advisor failures went GREEN — twice (boostertickets 2026-08-04
via upgrade-app; saa-mappings 2026-09-15 via create-mapping: a green run opened a PR deleting
real version blocks). build-mapping kept the bare path until #7.

The step's `if:` is evaluated by the platform, never by PR CI, so this test evaluates the
hashFiles patterns from the parsed YAML against a fixture tree with a files-only glob (the
property both incidents turned on), then executes the step body against the same tree.
"""
import glob
import os
import pathlib
import re
import subprocess
import sys
import tempfile

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
STEP = "Get errors if exist"
# workflow file -> where its advisor run writes .advisor/errors (relative to the workspace)
ERROR_DIRS = {
    "build-mapping.yml": [".advisor/errors"],                 # CLI runs from the checkout root
    "create-mapping.yml": ["ignore/.advisor/errors"],         # CLI runs from the ignore/ clone
    "upgrade-app.yml": [".advisor/errors", "app/.advisor/errors"],  # root, or a saa-path subdir
}

failures = []


def fail(msg):
    failures.append(msg)
    print(f"FAIL: {msg}")


def errors_step(fname):
    wf = yaml.safe_load((WORKFLOWS / fname).read_text())
    found = [s for job in wf["jobs"].values() for s in job.get("steps", []) if s.get("name") == STEP]
    if len(found) != 1:
        fail(f"{fname}: expected one '{STEP}' step, found {len(found)}")
        return None
    return found[0]


def hash_patterns(cond):
    call = re.search(r"hashFiles\(([^)]*)\)", cond or "")
    return re.findall(r"'([^']*)'", call.group(1)) if call else []


def hashfiles_matches(patterns, workspace):
    """Files (never directories) the patterns match — hashFiles hashes files, so a pattern that
    names only a directory hashes nothing and returns ''."""
    hits = set()
    for pat in patterns:
        for rel in glob.glob(pat, root_dir=workspace, recursive=True, include_hidden=True):
            if pathlib.Path(workspace, rel).is_file():
                hits.add(rel)
    return hits


def seed(workspace, errors_dir=None):
    # Every advisor run leaves a mapping/build output — none of it may trip the errors step.
    out = pathlib.Path(workspace, ".advisor/mappings")
    out.mkdir(parents=True)
    (out / "x.json").write_text("{}")
    if errors_dir:
        d = pathlib.Path(workspace, errors_dir)
        d.mkdir(parents=True)
        (d / "error-1.txt").write_text(f"ADVISOR-ERROR from {errors_dir}\n")


def main():
    cases = 0
    for fname, dirs in ERROR_DIRS.items():
        step = errors_step(fname)
        if step is None:
            continue
        cond = step.get("if", "")
        if "always()" not in cond:
            fail(f"{fname}: '{STEP}' if: lacks always() — a failed earlier step would skip the backstop: {cond!r}")
        patterns = hash_patterns(cond)
        if not patterns:
            fail(f"{fname}: '{STEP}' if: carries no hashFiles(...) patterns: {cond!r}")
            continue

        with tempfile.TemporaryDirectory() as clean:
            seed(clean)
            cases += 1
            hits = hashfiles_matches(patterns, clean)
            if hits:
                fail(f"{fname}: {patterns} match {sorted(hits)} with no errors written — the step would fire on every run")
            else:
                print(f"ok: {fname} [no errors written: step skipped]")

        for errors_dir in dirs:
            with tempfile.TemporaryDirectory() as ws:
                seed(ws, errors_dir)
                cases += 1
                if not hashfiles_matches(patterns, ws):
                    fail(f"{fname} [{errors_dir}]: hashFiles{tuple(patterns)} matches no file — advisor errors would read green")
                    continue
                # GitHub runs `run:` bodies with `bash -e {0}` when no shell is set.
                p = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=ws, capture_output=True, text=True,
                                   timeout=30, env={"PATH": os.environ["PATH"], "HOME": ws})
                out = p.stdout + p.stderr
                if p.returncode == 0 or f"ADVISOR-ERROR from {errors_dir}" not in out:
                    fail(f"{fname} [{errors_dir}]: step body exit {p.returncode}, want non-zero printing the error\n{out[-400:]}")
                else:
                    print(f"ok: {fname} [{errors_dir}: step fires, prints the error, exit {p.returncode}]")

    if failures:
        print(f"{len(failures)} failure(s)")
        return 1
    print(f"test_errors_step: OK — {cases} executed cases across {len(ERROR_DIRS)} workflows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
