#!/usr/bin/env python3
"""
Find the source repository URL for a Maven artifact.
Works on both macOS and Linux (including GitHub runners).
Enhanced to handle Apache projects specially.
"""

import sys
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
import json
import re

def fetch_url(url):
    """Fetch URL content using urllib (no external dependencies)."""
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None

def clean_scm_url(url):
    """Clean various SCM URL formats to standard HTTPS URLs."""
    if not url:
        return None

    if '--verbose' in sys.argv:
        print(f"Starting url before cleaning: {url}", file=sys.stderr)

    # Remove SCM prefixes
    url = url.replace('scm:', '')

    # Convert various formats to HTTPS
    url = url.replace('git://', 'https://')
    url = url.replace('ssh://git@', 'https://')
    url = url.replace('git@github.com:', 'https://github.com/')
    url = url.replace('git@gitlab.com:', 'https://gitlab.com/')
    url = url.replace('git@bitbucket.org:', 'https://bitbucket.org/')

    # remove prefix of git if exists
    url = url.replace('git:', '')

    # Remove .git suffix and anything after it
    url = re.sub(re.escape(".git") + r'.*', '', url)

    # Remove trailing slashes
    url = url.rstrip('/')

    return url

def get_repo_url(group_id, artifact_id, max_depth=10):
    """Find repository URL by traversing parent POMs."""

    group_path = group_id.replace('.', '/')

    # Step 1: Get latest version
    metadata_url = f"https://repo1.maven.org/maven2/{group_path}/{artifact_id}/maven-metadata.xml"

    if '--verbose' in sys.argv:
        print(f"Fetching metadata: {metadata_url}", file=sys.stderr)

    metadata = fetch_url(metadata_url)
    if not metadata:
        print(f"Failed to fetch metadata for {group_id}:{artifact_id}", file=sys.stderr)
        return None

    try:
        root = ET.fromstring(metadata)
        version_elem = root.find('.//latest')
        if version_elem is None:
            version_elem = root.find('.//release')
        if version_elem is None:
            # Try to get the last version from versions list
            versions = root.find('.//versions')
            if versions is not None:
                version_list = [v.text for v in versions.findall('version')]
                if version_list:
                    version = version_list[-1]
                else:
                    print("No version found in metadata", file=sys.stderr)
                    return None
            else:
                print("No version found in metadata", file=sys.stderr)
                return None
        else:
            version = version_elem.text

        if '--verbose' in sys.argv:
            print(f"Found version: {version}", file=sys.stderr)

    except ET.ParseError as e:
        print(f"Error parsing metadata XML: {e}", file=sys.stderr)
        return None

    # Step 2: Traverse POMs looking for SCM info
    current_group = group_id
    current_artifact = artifact_id
    current_version = version
    depth = 0

    while depth < max_depth:
        group_path = current_group.replace('.', '/')
        pom_url = f"https://repo1.maven.org/maven2/{group_path}/{current_artifact}/{current_version}/{current_artifact}-{current_version}.pom"

        if '--verbose' in sys.argv:
            print(f"Fetching POM (depth {depth}): {pom_url}", file=sys.stderr)

        pom_content = fetch_url(pom_url)
        if not pom_content:
            print(f"Failed to fetch POM: {pom_url}", file=sys.stderr)
            break

        try:
            root = ET.fromstring(pom_content)

            # Remove namespace for easier parsing
            for elem in root.iter():
                if '}' in elem.tag:
                    elem.tag = elem.tag.split('}')[1]

            # Check for SCM section
            scm = root.find('.//scm')
            if scm is not None:
                # Try different SCM fields
                for field in ['developerConnection', 'url', 'connection']:
                    elem = scm.find(field)
                    if elem is not None and elem.text:
                        cleaned_url = clean_scm_url(elem.text)
                        if cleaned_url:
                            if '--verbose' in sys.argv:
                                print(f"Found SCM URL in {field}: {cleaned_url}", file=sys.stderr)
                            return cleaned_url

            # Check project URL as fallback
            url_elem = root.find('.//url')
            if url_elem is not None and url_elem.text:
                url = url_elem.text.strip()
                # Check if it's a repo URL
                if any(host in url for host in ['github.com', 'gitlab.com', 'bitbucket.org', 'sourceforge.net']):
                    if '--verbose' in sys.argv:
                        print(f"Found project URL: {url}", file=sys.stderr)
                    return url

            # Continue to parent if exists
            if parent is not None:
                parent_group = parent.find('groupId')
                parent_artifact = parent.find('artifactId')
                parent_version = parent.find('version')

                if (parent_group is not None and parent_group.text == 'org.sonatype.oss' and
                        parent_artifact is not None and parent_artifact.text == 'oss-parent'):

                    if '--verbose' in sys.argv:
                        print(f"Found Sonatype root parent, stopping traversal as there is no information available", file=sys.stderr)

                    return None

                    # Check if the parent is the Apache root POM
                if (parent_group is not None and parent_group.text == 'org.apache' and
                        parent_artifact is not None and parent_artifact.text == 'apache'):

                    if '--verbose' in sys.argv:
                        print(f"Found Apache root parent, stopping traversal", file=sys.stderr)

                    # Get the artifactId from the current POM (not the parent)
                    current_artifact_elem = root.find('artifactId')
                    if current_artifact_elem is not None and current_artifact_elem.text:
                        project_name = current_artifact_elem.text

                        # Construct and return the GitHub URL
                        github_url = f"https://github.com/apache/{project_name}"

                        if '--verbose' in sys.argv:
                            print(f"Using current POM artifactId: {project_name}", file=sys.stderr)
                            print(f"Returning Apache GitHub URL: {github_url}", file=sys.stderr)

                        return github_url

                if all(elem is not None and elem.text for elem in [parent_group, parent_artifact, parent_version]):
                    current_group = parent_group.text
                    current_artifact = parent_artifact.text
                    current_version = parent_version.text

                    if '--verbose' in sys.argv:
                        print(f"Following parent: {current_group}:{current_artifact}:{current_version}", file=sys.stderr)

                    depth += 1
                else:
                    break
            else:
                if '--verbose' in sys.argv:
                    print("No parent POM found", file=sys.stderr)
                break

        except ET.ParseError as e:
            print(f"Error parsing POM: {e}", file=sys.stderr)
            break

    return None

def main():
    if len(sys.argv) < 2 or ':' not in sys.argv[1]:
        print("Usage: ./get-repo-url.py groupId:artifactId [--verbose]")
        print("Example: ./get-repo-url.py org.jolokia:jolokia-support-spring")
        print("         ./get-repo-url.py net.javacrumbs.shedlock:shedlock-spring --verbose")
        print("         ./get-repo-url.py org.apache.camel:camel-spring-main --verbose")
        sys.exit(1)

    group_id, artifact_id = sys.argv[1].split(':', 1)
    repo_url = get_repo_url(group_id, artifact_id)

    if repo_url:
        print(repo_url)
        sys.exit(0)
    else:
        print(f"Repository URL not found for {group_id}:{artifact_id}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()