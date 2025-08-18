#!/usr/bin/env python3
"""
Script to trigger GitHub workflows based on resolved artifacts from artifact-details.json
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple
from datetime import datetime

def load_artifact_details(filepath: str = "artifact-details.json") -> Dict:
    """Load and parse the artifact details JSON file."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filepath} not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        sys.exit(1)

def filter_resolved_artifacts(data: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    Separate artifacts into resolved and unresolved lists.

    Returns:
        Tuple of (resolved_artifacts, unresolved_artifacts)
    """
    resolved = []
    unresolved = []

    for artifact in data.get("artifacts", []):
        if artifact.get("resolved") is True:
            resolved.append(artifact)
        else:
            unresolved.append(artifact)

    return resolved, unresolved

def get_unique_repositories(artifacts: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Filter artifacts to unique repository URLs.
    If multiple artifacts share the same repository_url, keep only the first.

    Returns:
        Tuple of (unique_artifacts, duplicate_artifacts)
    """
    seen_urls: Set[str] = set()
    unique_artifacts: List[Dict] = []
    duplicate_artifacts: List[Dict] = []

    for artifact in artifacts:
        repo_url = artifact.get("repository_url", "")
        if repo_url:
            if repo_url not in seen_urls:
                seen_urls.add(repo_url)
                unique_artifacts.append(artifact)
            else:
                duplicate_artifacts.append(artifact)

    return unique_artifacts, duplicate_artifacts

def trigger_workflow(artifact: Dict, target_repo: str = "org/repo",
                     workflow_file: str = "generate-mapping-workflow.yml",
                     ref: str = "main", dry_run: bool = False,
                     delay_seconds: int = 0) -> bool:
    """
    Trigger a GitHub workflow using gh CLI.

    Args:
        artifact: The artifact data containing the required fields
        target_repo: The target repository (org/repo format)
        workflow_file: The workflow file name
        ref: The branch/tag/SHA to run the workflow from
        dry_run: If True, print the command instead of executing
        delay_seconds: Seconds to wait after triggering (for rate limiting)

    Returns:
        True if successful, False otherwise
    """
    # Prepare the workflow inputs
    slug = artifact.get("artifact_id", "")
    repo_url = artifact.get("repository_url", "")
    coordinates = artifact.get("artifact", "")

    # Build the gh workflow run command
    cmd = [
        "gh", "workflow", "run",
        workflow_file,
        "--repo", target_repo,
        "--ref", ref,
        "--field", f"slug={slug}",
        "--field", f"repo_url={repo_url}",
        "--field", f"coordinates={coordinates}"
    ]

    if dry_run:
        print(f"[DRY RUN] Would execute: {' '.join(cmd)}")
        if delay_seconds > 0:
            print(f"[DRY RUN] Would wait {delay_seconds} second(s) after trigger")
        return True

    try:
        print(f"Triggering workflow for artifact: {coordinates}")
        print(f"  Repository: {repo_url}")
        print(f"  Slug: {slug}")

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✓ Successfully triggered workflow for {coordinates}")

        # Apply rate limit delay if specified
        if delay_seconds > 0:
            print(f"  Waiting {delay_seconds} second(s) before next trigger (rate limit protection)...")
            time.sleep(delay_seconds)

        return True

    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to trigger workflow for {coordinates}")
        print(f"  Error: {e.stderr}")
        return False

def print_summary_section(title: str, char: str = "="):
    """Print a formatted section header."""
    width = 80
    print("\n" + char * width)
    print(f" {title}")
    print(char * width)

def format_artifact_info(artifact: Dict) -> str:
    """Format artifact information for display."""
    return (f"  • {artifact.get('artifact', 'N/A')}\n"
            f"    Repository: {artifact.get('repository_url', 'N/A')}\n"
            f"    Error: {artifact.get('error', 'N/A')}")

def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(description="Trigger workflows for resolved artifacts")
    parser.add_argument("--file", default="artifact-details.json",
                        help="Path to artifact-details.json (default: artifact-details.json)")
    parser.add_argument("--repo", default="org/repo",
                        help="Target repository in org/repo format (default: org/repo)")
    parser.add_argument("--workflow", default="generate-mapping-workflow.yml",
                        help="Workflow file name (default: generate-mapping-workflow.yml)")
    parser.add_argument("--ref", default="main",
                        help="Git ref to run workflow from (default: main)")
    parser.add_argument("--delay", type=int, default=5,
                        help="Seconds to wait between workflow triggers to avoid rate limits (default: 5, use 0 to disable)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing them")

    args = parser.parse_args()

    # Track all processing results
    processing_results = {
        "total_artifacts": 0,
        "resolved_count": 0,
        "unresolved_count": 0,
        "unique_repos": 0,
        "duplicate_repos": 0,
        "workflows_triggered": 0,
        "workflows_failed": 0,
        "triggered_artifacts": [],
        "failed_artifacts": [],
        "unresolved_artifacts": [],
        "duplicate_artifacts": []
    }

    # Load the artifact details
    print(f"Loading artifact details from {args.file}...")
    data = load_artifact_details(args.file)

    # Get total count
    all_artifacts = data.get("artifacts", [])
    processing_results["total_artifacts"] = len(all_artifacts)

    # Separate resolved and unresolved artifacts
    resolved_artifacts, unresolved_artifacts = filter_resolved_artifacts(data)
    processing_results["resolved_count"] = len(resolved_artifacts)
    processing_results["unresolved_count"] = len(unresolved_artifacts)
    processing_results["unresolved_artifacts"] = unresolved_artifacts

    print(f"Found {len(resolved_artifacts)} resolved artifacts")
    print(f"Found {len(unresolved_artifacts)} unresolved artifacts")

    # Filter to unique repository URLs
    unique_artifacts, duplicate_artifacts = get_unique_repositories(resolved_artifacts)
    processing_results["unique_repos"] = len(unique_artifacts)
    processing_results["duplicate_repos"] = len(duplicate_artifacts)
    processing_results["duplicate_artifacts"] = duplicate_artifacts

    print(f"Found {len(unique_artifacts)} unique repositories")
    if duplicate_artifacts:
        print(f"Skipping {len(duplicate_artifacts)} duplicate repository entries")

    # Trigger workflows if we have unique artifacts
    if unique_artifacts:
        print_summary_section("TRIGGERING WORKFLOWS", "=")

        if args.delay > 0 and len(unique_artifacts) > 1:
            total_delay = args.delay * (len(unique_artifacts) - 1)
            print(f"Note: Will add {args.delay}s delay between triggers (total ~{total_delay}s)")
            print()

        for i, artifact in enumerate(unique_artifacts):
            # Don't delay after the last workflow
            delay = args.delay if i < len(unique_artifacts) - 1 else 0

            if trigger_workflow(artifact, args.repo, args.workflow, args.ref,
                                args.dry_run, delay):
                processing_results["workflows_triggered"] += 1
                processing_results["triggered_artifacts"].append(artifact)
            else:
                processing_results["workflows_failed"] += 1
                processing_results["failed_artifacts"].append(artifact)
            print("-" * 40)

    # Print comprehensive summary
    print_summary_section("PROCESSING SUMMARY", "═")

    # Overall statistics
    print("\n📊 OVERALL STATISTICS:")
    print(f"  Total artifacts processed: {processing_results['total_artifacts']}")
    print(f"  Resolved artifacts: {processing_results['resolved_count']}")
    print(f"  Unresolved artifacts: {processing_results['unresolved_count']}")
    print(f"  Unique repositories: {processing_results['unique_repos']}")
    print(f"  Duplicate repositories skipped: {processing_results['duplicate_repos']}")

    # Workflow execution results
    print("\n🚀 WORKFLOW EXECUTION:")
    print(f"  Workflows triggered successfully: {processing_results['workflows_triggered']}")
    print(f"  Workflows failed: {processing_results['workflows_failed']}")

    # Detailed successful mappings
    if processing_results["triggered_artifacts"]:
        print("\n✅ SUCCESSFULLY GENERATED MAPPINGS:")
        for artifact in processing_results["triggered_artifacts"]:
            print(f"\n  {artifact.get('artifact', 'N/A')}")
            print(f"    • Repository: {artifact.get('repository_url', 'N/A')}")
            print(f"    • Slug: {artifact.get('artifact_id', 'N/A')}")
            print(f"    • Coordinates: {artifact.get('artifact', 'N/A')}")

    # Failed workflows (if any)
    if processing_results["failed_artifacts"]:
        print("\n❌ FAILED WORKFLOW TRIGGERS:")
        for artifact in processing_results["failed_artifacts"]:
            print(format_artifact_info(artifact))

    # Unresolved artifacts that need manual attention
    if processing_results["unresolved_artifacts"]:
        print("\n⚠️  UNRESOLVED ARTIFACTS (REQUIRES MANUAL RESOLUTION):")
        print(f"  Found {len(processing_results['unresolved_artifacts'])} artifacts that could not be resolved automatically:")
        for artifact in processing_results["unresolved_artifacts"]:
            print(f"\n{format_artifact_info(artifact)}")

    # Duplicate artifacts that were skipped
    if processing_results["duplicate_artifacts"]:
        print("\n📋 SKIPPED DUPLICATE REPOSITORY ARTIFACTS:")
        for artifact in processing_results["duplicate_artifacts"]:
            print(f"  • {artifact.get('artifact', 'N/A')} (duplicate of {artifact.get('repository_url', 'N/A')})")

    # Final status
    print_summary_section("FINAL STATUS", "═")

    if processing_results["workflows_failed"] > 0:
        print("❌ COMPLETED WITH ERRORS")
        print(f"   {processing_results['workflows_failed']} workflow(s) failed to trigger")
        exit_code = 1
    elif processing_results["workflows_triggered"] == 0 and processing_results["unique_repos"] > 0:
        print("⚠️  NO WORKFLOWS TRIGGERED")
        print("   Check your configuration and permissions")
        exit_code = 1
    elif processing_results["unresolved_count"] > 0:
        print("⚠️  COMPLETED WITH WARNINGS")
        print(f"   {processing_results['workflows_triggered']} workflow(s) triggered successfully")
        print(f"   {processing_results['unresolved_count']} artifact(s) require manual resolution")
        exit_code = 0
    else:
        print("✅ COMPLETED SUCCESSFULLY")
        print(f"   All {processing_results['workflows_triggered']} workflow(s) triggered successfully")
        exit_code = 0

    # GitHub Actions summary (if running in GitHub Actions)
    if os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_STEP_SUMMARY'):
        write_github_summary(processing_results)

    print("\n" + "═" * 80)
    print(f"Execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return exit_code

def write_github_summary(results: Dict):
    """Write a summary to GitHub Actions step summary."""
    import os

    summary_file = os.environ.get('GITHUB_STEP_SUMMARY')
    if not summary_file:
        return

    with open(summary_file, 'a') as f:
        f.write("## 📊 Artifact Processing Summary\n\n")

        # Statistics table
        f.write("### Overall Statistics\n")
        f.write("| Metric | Count |\n")
        f.write("|--------|-------|\n")
        f.write(f"| Total Artifacts | {results['total_artifacts']} |\n")
        f.write(f"| Resolved | {results['resolved_count']} |\n")
        f.write(f"| Unresolved | {results['unresolved_count']} |\n")
        f.write(f"| Unique Repositories | {results['unique_repos']} |\n")
        f.write(f"| Workflows Triggered | {results['workflows_triggered']} |\n")
        f.write(f"| Workflows Failed | {results['workflows_failed']} |\n\n")

        # Unresolved artifacts
        if results['unresolved_artifacts']:
            f.write("### ⚠️ Unresolved Artifacts (Manual Action Required)\n\n")
            for artifact in results['unresolved_artifacts']:
                f.write(f"- **{artifact.get('artifact', 'N/A')}**\n")
                f.write(f"  - Error: `{artifact.get('error', 'N/A')}`\n")

        # Success status
        if results['workflows_failed'] == 0 and results['workflows_triggered'] > 0:
            f.write("\n✅ **All workflows triggered successfully!**\n")
        elif results['workflows_failed'] > 0:
            f.write(f"\n❌ **{results['workflows_failed']} workflow(s) failed to trigger**\n")

if __name__ == "__main__":
    import os
    sys.exit(main())