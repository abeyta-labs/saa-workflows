#!/usr/bin/env python3
"""
Maven Repository Lookup Tool
Resolves source repository URLs for Maven artifacts from an input file.
"""

import sys
import json
import csv
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse
import concurrent.futures
import os

def resolve_artifact(artifact: str, resolver_script: str = "./get-repo-url.py") -> Dict[str, Any]:
    """Resolve a single artifact and return results."""
    parts = artifact.split(':')
    if len(parts) != 2:
        return {
            'artifact': artifact,
            'group_id': '',
            'artifact_id': '',
            'resolved': False,
            'repository_url': '',
            'error': 'Invalid artifact format',
            'response_time_ms': 0
        }

    group_id, artifact_id = parts

    start_time = time.time()
    try:
        result = subprocess.run(
            [sys.executable, resolver_script, artifact],
            capture_output=True,
            text=True,
            timeout=30
        )
        elapsed_ms = int((time.time() - start_time) * 1000)

        if result.returncode == 0:
            return {
                'artifact': artifact,
                'group_id': group_id,
                'artifact_id': artifact_id,
                'resolved': True,
                'repository_url': result.stdout.strip(),
                'error': None,
                'response_time_ms': elapsed_ms
            }
        else:
            error_msg = result.stderr.strip() if result.stderr else 'Repository URL not found'
            return {
                'artifact': artifact,
                'group_id': group_id,
                'artifact_id': artifact_id,
                'resolved': False,
                'repository_url': '',
                'error': error_msg,
                'response_time_ms': elapsed_ms
            }
    except subprocess.TimeoutExpired:
        return {
            'artifact': artifact,
            'group_id': group_id,
            'artifact_id': artifact_id,
            'resolved': False,
            'repository_url': '',
            'error': 'Resolution timeout (30s)',
            'response_time_ms': 30000
        }
    except Exception as e:
        return {
            'artifact': artifact,
            'group_id': group_id,
            'artifact_id': artifact_id,
            'resolved': False,
            'repository_url': '',
            'error': str(e),
            'response_time_ms': 0
        }

def read_artifacts(file_path: str) -> List[str]:
    """Read artifacts from input file, skipping comments and empty lines."""
    artifacts = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                artifacts.append(line)
    return artifacts

def resolve_parallel(artifacts: List[str], resolver_script: str, max_workers: int) -> List[Dict[str, Any]]:
    """Resolve artifacts in parallel."""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_artifact = {
            executor.submit(resolve_artifact, artifact, resolver_script): artifact
            for artifact in artifacts
        }

        for future in concurrent.futures.as_completed(future_to_artifact):
            result = future.result()
            results.append(result)

            # Show progress if stderr is a terminal
            if sys.stderr.isatty():
                resolved_count = sum(1 for r in results if r['resolved'])
                sys.stderr.write(f"\rProcessing: {len(results)}/{len(artifacts)} "
                                 f"(Resolved: {resolved_count})  ")
                sys.stderr.flush()

    if sys.stderr.isatty():
        sys.stderr.write("\r" + " " * 50 + "\r")  # Clear progress line
        sys.stderr.flush()

    # Sort results to maintain input order
    artifact_order = {a: i for i, a in enumerate(artifacts)}
    results.sort(key=lambda r: artifact_order.get(r['artifact'], float('inf')))

    return results

def output_json(results: List[Dict], input_file: str, output_file: Optional[str] = None):
    """Output results in JSON format."""
    output = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'input_file': input_file,
        'artifacts': results,
        'summary': {
            'total': len(results),
            'resolved': sum(1 for r in results if r['resolved']),
            'unresolved': sum(1 for r in results if not r['resolved']),
            'resolution_rate': round(
                sum(1 for r in results if r['resolved']) * 100 / len(results), 2
            ) if results else 0,
            'avg_response_time_ms': round(
                sum(r['response_time_ms'] for r in results) / len(results), 2
            ) if results else 0
        }
    }

    json_str = json.dumps(output, indent=2)
    if output_file:
        with open(output_file, 'w') as f:
            f.write(json_str)
    else:
        print(json_str)

def output_csv(results: List[Dict], output_file: Optional[str] = None):
    """Output results in CSV format."""
    fieldnames = ['artifact', 'group_id', 'artifact_id', 'resolved',
                  'repository_url', 'error', 'response_time_ms']

    if output_file:
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    else:
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

def output_markdown(results: List[Dict], input_file: str):
    """Output results in Markdown format."""
    print("# Maven Artifact Repository URLs")
    print(f"\n**Input File:** `{input_file}`")
    print(f"**Timestamp:** {datetime.utcnow().isoformat()}Z")
    print("\n| Artifact | Status | Repository URL | Response Time |")
    print("|----------|--------|----------------|---------------|")

    for r in results:
        status = "✅" if r['resolved'] else "❌"
        url = r['repository_url'] or (r.get('error', 'Not found'))
        print(f"| {r['artifact']} | {status} | {url} | {r['response_time_ms']}ms |")

    total = len(results)
    resolved = sum(1 for r in results if r['resolved'])
    print(f"\n**Summary:** {resolved}/{total} resolved "
          f"({resolved*100//total if total else 0}%)")

def output_table(results: List[Dict]):
    """Output results in table format with colors."""
    # ANSI color codes
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'

    # Disable colors if not a terminal
    if not sys.stdout.isatty():
        GREEN = RED = RESET = ''

    print(f"{'ARTIFACT':<60} {'STATUS':<10} {'REPOSITORY URL':<50} {'TIME(ms)':<10}")
    print("-" * 130)

    for r in results:
        if r['resolved']:
            color = GREEN
            status = "✓"
            url = r['repository_url']
        else:
            color = RED
            status = "✗"
            url = r.get('error', 'Not found')

        print(f"{color}{r['artifact']:<60} {status:<10} {url:<50} "
              f"{r['response_time_ms']:<10}{RESET}")

    total = len(results)
    resolved = sum(1 for r in results if r['resolved'])
    unresolved = total - resolved

    print("-" * 130)
    print(f"Summary: Total: {total} | Resolved: {resolved} | "
          f"Unresolved: {unresolved} | Resolution Rate: "
          f"{resolved*100//total if total else 0}%")

def main():
    parser = argparse.ArgumentParser(
        description='Resolve Maven artifact repository URLs from input file'
    )
    parser.add_argument('input_file', nargs='?', default='artifacts.txt',
                        help='File containing artifacts (one per line)')
    parser.add_argument('-f', '--format',
                        choices=['json', 'csv', 'markdown', 'table'],
                        default='json',
                        help='Output format (default: json)')
    parser.add_argument('-o', '--output',
                        help='Output file (stdout if not specified)')
    parser.add_argument('-r', '--resolver', default='./get-repo-url.py',
                        help='Path to get-repo-url.py resolver script')
    parser.add_argument('-p', '--parallel', type=int, default=4,
                        help='Number of parallel workers (default: 4)')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Suppress progress output')

    args = parser.parse_args()

    # Validate input file
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)

    # Validate resolver script
    if not Path(args.resolver).exists():
        print(f"Error: Resolver script '{args.resolver}' not found", file=sys.stderr)
        sys.exit(1)

    # Read artifacts from input file
    artifacts = read_artifacts(args.input_file)

    if not artifacts:
        print("Warning: No artifacts found in input file", file=sys.stderr)
        # Output empty result
        if args.format == 'json':
            output_json([], args.input_file, args.output)
        sys.exit(0)

    # Resolve artifacts
    if not args.quiet:
        print(f"Resolving {len(artifacts)} artifacts using "
              f"{args.parallel} workers...", file=sys.stderr)

    results = resolve_parallel(artifacts, args.resolver, args.parallel)

    # Output results in requested format
    if args.format == 'json':
        output_json(results, args.input_file, args.output)
    elif args.format == 'csv':
        output_csv(results, args.output)
    elif args.format == 'markdown':
        output_markdown(results, args.input_file)
    else:  # table
        output_table(results)

if __name__ == '__main__':
    main()