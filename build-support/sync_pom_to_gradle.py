#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A Python script to parse a set of Maven pom.xml files from a multi-module
project. It extracts version properties, dependency management information,
submodule dependencies, and any dependency exclusions. The result is then
output in a structured JSON format.

It can also sync this information into corresponding build.gradle.kts files.

under dir {root}/fe, run:

Usage for Parsing:
  python sync_pom_to_gradle.py

Usage for Syncing to Gradle:
  python sync_pom_to_gradle.py --sync-gradle
"""

import sys
import json
import os
import re
import xml.etree.ElementTree as ET

# --- XML Parsing Logic (from previous steps) ---

POM_NAMESPACE = {'m': 'http://maven.apache.org/POM/4.0.0'}

def get_child_text(element, tag_name, namespace_map):
    child = element.find(tag_name, namespace_map)
    return child.text.strip() if child is not None and child.text else None

def parse_properties(root, ns):
    properties_node = root.find('m:properties', ns)
    if properties_node is None:
        return None
    vars_list = []
    for prop in properties_node:
        key = prop.tag.replace(f'{{{ns["m"]}}}', '')
        value = prop.text
        if key and value:
            vars_list.append({'name': key, 'version': value.strip()})
    return vars_list if vars_list else None

def _parse_exclusions(dependency_node, ns):
    exclusions_node = dependency_node.find('m:exclusions', ns)
    if exclusions_node is None:
        return None
    exclusions_list = []
    for exclusion_node in exclusions_node.findall('m:exclusion', ns):
        group_id = get_child_text(exclusion_node, 'm:groupId', ns)
        artifact_id = get_child_text(exclusion_node, 'm:artifactId', ns)
        if group_id and artifact_id:
            exclusions_list.append(f"{group_id}:{artifact_id}")
    return exclusions_list if exclusions_list else None

def _parse_dependency_list(dependency_nodes, ns):
    deps_dict = {}
    for dep_node in dependency_nodes:
        group_id = get_child_text(dep_node, 'm:groupId', ns)
        artifact_id = get_child_text(dep_node, 'm:artifactId', ns)
        if not group_id or not artifact_id:
            continue
        key = f"{group_id}:{artifact_id}"
        dep_info = {}
        optional_fields = ['version', 'type', 'scope', 'classifier']
        for field in optional_fields:
            value = get_child_text(dep_node, f'm:{field}', ns)
            if value:
                dep_info[field] = value
        exclusions = _parse_exclusions(dep_node, ns)
        if exclusions:
            dep_info['exclusions'] = exclusions
        deps_dict[key] = dep_info
    return deps_dict if deps_dict else None

def parse_dependency_management(root, ns):
    dep_man_nodes = root.findall('m:dependencyManagement/m:dependencies/m:dependency', ns)
    return _parse_dependency_list(dep_man_nodes, ns) if dep_man_nodes else None

def parse_dependencies(root, ns):
    dep_nodes = root.findall('m:dependencies/m:dependency', ns)
    return _parse_dependency_list(dep_nodes, ns) if dep_nodes else None


# --- Gradle Syncing Logic ---

def _replace_content_between_markers(content, start_marker, end_marker, new_block):
    """
    Replaces content within a file between start and end markers.
    The markers themselves are preserved.
    """
    # Pattern to find the block, including the markers themselves.
    # It captures the start marker line and the end marker line to preserve them.
    pattern = re.compile(
        f'({re.escape(start_marker)}\\n).*?(\\n\\s*{re.escape(end_marker)})',
        re.DOTALL
    )
    # The replacement consists of the captured start marker, the new content,
    # and the captured end marker.
    replacement = f'\\1{new_block}\\2'

    # Perform the substitution
    new_content, count = pattern.subn(replacement, content)

    if count == 0:
        print(f"Warning: Markers '{start_marker}' and '{end_marker}' not found. No changes made to this block.", file=sys.stderr)
        return content # Return original content if markers not found

    return new_content

def _generate_gradle_vars_string(vars_list, indent="        "):
    """
    Generates the body of the `ext { ... }` block for Gradle.
    """
    if not vars_list:
        return ""

    # Filter out properties that are usually handled manually in Gradle
    vars_to_skip = {
        'starrocks.home', 'project.build.sourceEncoding', 'skip.plugin',
        'sonar.organization', 'sonar.host.url'
    }

    lines = []
    for var in vars_list:
        name = var['name']
        version = var['version']
        if name not in vars_to_skip:
            # Escape backslashes and double quotes in version string
            escaped_version = version.replace('\\', '\\\\').replace('"', '\\"')
            lines.append(f'{indent}set("{name}", "{escaped_version}")')

    return "\n".join(lines)

def _resolve_gradle_version(version_str, vars_map):
    """
    Translates a Maven version string (literal or variable) to a Gradle version string.
    e.g., "${jackson.version}" -> '"${project.ext["jackson.version"]}"'
    """
    if not version_str:
        return '""' # Should not happen in constraints, but defensive

    match = re.match(r"\$\{(.*)\}", version_str)
    if match:
        var_name = match.group(1)
        return f'${{project.ext["{var_name}"]}}'
    else:
        # It's a literal version
        return version_str

def _generate_gradle_deps_string(deps_dict, vars_map, is_constraints, dep_management, indent="            "):
    """
    Generates the body of the `dependencies` or `constraints` block for Gradle.
    """
    if not deps_dict:
        return ""

    lines = []
    for key, info in sorted(deps_dict.items()):
        # Skip BOMs (Bill of Materials), as they are handled with platform()
        if info.get('type') == 'pom':
            continue

        if key == 'com.starrocks:jprotobuf-starrocks' and not is_constraints:
            # special handling manually
            continue

        merged_info = dep_management.get(key, {}).copy() if dep_management else {}
        merged_info.update(info)

        # Map Maven scope to Gradle configuration
        scope = merged_info.get('scope')
        if is_constraints or key == 'com.starrocks:jprotobuf-starrocks':
            config = "implementation"
        elif scope == 'test':
            config = 'testImplementation'
        elif scope == 'provided':
            config = 'compileOnly'
        elif scope == 'runtime':
            config = 'runtimeOnly'
        else:
            config = 'implementation'

        version_str = info.get('version')
        version_part = _resolve_gradle_version(version_str, vars_map) if version_str else ""

        # Build the core dependency string: group:artifact:version
        dep_string = f"{key}"
        if version_part and version_part != '""':
            dep_string += f":{version_part}"

        # Add classifier if it exists
#         classifier = merged_info.get('classifier')
#         if classifier:
#             if not version_part or version_part == '""':
#                 # Maven allows classifier without version, Gradle needs a placeholder
#                 dep_string += ":" # Add empty version part
#             dep_string += f":{classifier}"

        line = f'{indent}{config}("{dep_string}")'

        # Handle exclusions
        if 'exclusions' in merged_info and not is_constraints:
            line += " {\n"
            for exclusion in merged_info['exclusions']:
                group, module = exclusion.split(':', 1)
                line += f'{indent}    exclude(group = "{group}", module = "{module}")\n'
            line += f'{indent}}}'
        lines.append(line)

    return "\n".join(lines)

def sync_pom_to_gradle(pom_file, pom_data, dep_management=None):
    """
    Reads a build.gradle.kts file and replaces the content between markers
    with information parsed from the pom_data.
    """
    gradle_file = os.path.join(os.path.dirname(pom_file) or '.', 'build.gradle.kts')
    if not os.path.exists(gradle_file):
        # This is not an error, as not all modules may have a gradle file
        return

    print(f"--- Syncing {pom_file} to {gradle_file} ---")

    try:
        with open(gradle_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except IOError as e:
        print(f"Error reading gradle file {gradle_file}: {e}", file=sys.stderr)
        return

    original_content = content
    vars_map = {v['name']: v['version'] for v in pom_data.get('vars', [])}

    # Sync variables
    if 'vars' in pom_data:
        # sort list by name, and only include vars ending in ".version"
        pom_vars = sorted([v for v in pom_data['vars'] if v['name'].endswith('.version')], key=lambda x: x['name'])
        if len(pom_vars) > 0:
            new_vars_block = _generate_gradle_vars_string(pom_vars)
            content = _replace_content_between_markers(
                content,
                '// var sync start',
                '// var sync end',
                new_vars_block
            )

    # Sync dependencyManagement to the `constraints` block
    if 'dependencyManagement' in pom_data:
        new_deps_block = _generate_gradle_deps_string(
            pom_data['dependencyManagement'], vars_map, is_constraints=True, dep_management=dep_management
        )
        content = _replace_content_between_markers(
            content,
            '// dependency sync start',
            '// dependency sync end',
            new_deps_block
        )

    # Sync dependencies (for submodules)
    if 'dependency' in pom_data:
        new_deps_block = _generate_gradle_deps_string(
            pom_data['dependency'], vars_map, is_constraints=False, dep_management=dep_management, indent="    "
        )
        content = _replace_content_between_markers(
            content,
            '// dependency sync start',
            '// dependency sync end',
            new_deps_block
        )

    # Write back to the file only if changes were made
    if content != original_content:
        try:
            with open(gradle_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Successfully updated {gradle_file}")
        except IOError as e:
            print(f"Error writing to gradle file {gradle_file}: {e}", file=sys.stderr)
    else:
        print(f"No changes needed for {gradle_file}")


def main():
    """
    Main entry point. Parses CLI arguments and orchestrates the parsing
    and optional syncing.
    """
    args = sys.argv[1:]

    sync_mode = False
    if '--sync-gradle' in args:
        sync_mode = True
        args.remove('--sync-gradle')

    parent_dep_management = None
    all_poms_data = {}
    for pom_file in ['pom.xml', 'fe-core/pom.xml']:
        try:
            tree = ET.parse(pom_file)
            root = tree.getroot()
        except FileNotFoundError:
            print(f"Error: File not found: {pom_file}", file=sys.stderr)
            continue
        except ET.ParseError as e:
            print(f"Error: Could not parse XML file '{pom_file}': {e}", file=sys.stderr)
            continue

        pom_data = {}
        properties = parse_properties(root, POM_NAMESPACE)
        if properties: pom_data['vars'] = properties

        dep_management = parse_dependency_management(root, POM_NAMESPACE)
        if dep_management:
            pom_data['dependencyManagement'] = dep_management
            parent_dep_management = dep_management

        dependencies = parse_dependencies(root, POM_NAMESPACE)
        if dependencies: pom_data['dependency'] = dependencies

        if pom_data:
            all_poms_data[pom_file] = pom_data
            if sync_mode:
                if pom_file == 'pom.xml':
                    sync_pom_to_gradle(pom_file, pom_data, parent_dep_management)
                else:
                    sync_pom_to_gradle(pom_file, pom_data, parent_dep_management)

    if not sync_mode:
        print(json.dumps(all_poms_data, indent=4))

if __name__ == "__main__":
    main()