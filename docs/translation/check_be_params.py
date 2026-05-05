#!/usr/bin/env python3
"""
check_be_params.py  — Lint BE parameter documentation.

Checks:
  1. Parameters present in EN but missing from ZH or JA (and vice versa).
  2. Default values in any doc that disagree with config.h.
  3. Mutability flags in any doc that disagree with config.h.

Usage:
    python3 check_be_params.py --en <en.md> --zh <zh.md> [--ja <ja.md>] [--config <config.h>]

The --config argument defaults to be/src/common/config.h relative to the
repository root (two levels above the docs/ directory).

Exit code: 0 if no issues found, 1 if issues are found.
"""

import re
import sys
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ── source parser ──────────────────────────────────────────────────────────────

@dataclass
class SrcParam:
    name: str
    default: str
    mutable: bool
    type_name: str


# Matches: CONF_[m](Bool|Int32|Int64|Double|String_enum|Strings|String)(name, "default")
_CONF_RE = re.compile(
    r'CONF_(m?)'
    r'(Bool|Int32|Int64|Double|String_enum|Strings|String)'
    r'\s*\(\s*(\w+)\s*,\s*"([^"]*)"'
)
_ALIAS_RE = re.compile(r'CONF_Alias\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)')


def parse_config_h(path: Path) -> Tuple[Dict[str, SrcParam], Dict[str, str]]:
    """Return (params, aliases) from a config.h file."""
    text = path.read_text()
    params: Dict[str, SrcParam] = {}
    aliases: Dict[str, str] = {}  # alias_name -> canonical_name

    for m in _CONF_RE.finditer(text):
        mutable_flag, type_name, name, default = m.groups()
        params[name] = SrcParam(
            name=name,
            default=default,
            mutable=bool(mutable_flag),
            type_name=type_name,
        )
    for m in _ALIAS_RE.finditer(text):
        aliases[m.group(1)] = m.group(2)

    return params, aliases


# ── doc parser ─────────────────────────────────────────────────────────────────

@dataclass
class DocParam:
    name: str
    default: Optional[str] = None
    mutable: Optional[bool] = None


# Default field labels by language:
#   EN: "Default:"   ZH: "默认值："   JA: "デフォルト:"
_DEFAULT_RE = re.compile(r'^-\s+(?:Default|默认值|デフォルト)[：:]\s*(.+)$')

# Mutable field labels by language:
#   EN: "Is mutable:"   ZH: "是否动态："   JA: "変更可能:"
# Truthy values:  yes (EN), 是 (ZH), はい (JA)
# Falsy values:   no (EN),  否 (ZH), いいえ (JA)
# (The JA doc also uses bare "Yes"/"No" in some entries.)
_MUTABLE_RE = re.compile(r'^-\s+(?:Is mutable|是否动态|変更可能)[：:]\s*(.+)$')
_MUTABLE_TRUE = {'yes', '是', 'はい'}


def parse_doc(path: Path) -> Dict[str, DocParam]:
    """Return dict of param-name -> DocParam parsed from a markdown doc."""
    params: Dict[str, DocParam] = {}
    current: Optional[DocParam] = None

    for line in path.read_text().splitlines():
        m = re.match(r'^###\s+(\S+)\s*$', line)
        if m:
            current = DocParam(name=m.group(1))
            params[current.name] = current
            continue

        if current is None:
            continue

        m = _DEFAULT_RE.match(line)
        if m:
            current.default = m.group(1).strip()
            continue

        m = _MUTABLE_RE.match(line)
        if m:
            current.mutable = m.group(1).strip().lower() in _MUTABLE_TRUE
            continue

    return params


# ── comparison helpers ─────────────────────────────────────────────────────────

def normalize_default(val: Optional[str]) -> Optional[str]:
    """Normalise a default value for comparison; return None to skip."""
    if val is None:
        return None
    val = val.strip().strip('`')
    if val == '-':       # "-" in docs means "not documented" — skip
        return None
    if val.lower() in ('true', 'false'):
        return val.lower()
    return val


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_config = script_dir.parent.parent / 'be' / 'src' / 'common' / 'config.h'

    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('--en', required=True, metavar='FILE', help='English doc markdown file')
    ap.add_argument('--zh', required=True, metavar='FILE', help='Chinese doc markdown file')
    ap.add_argument('--ja', metavar='FILE', help='Japanese doc markdown file (optional)')
    ap.add_argument(
        '--config',
        default=str(default_config),
        metavar='FILE',
        help=f'config.h path (default: {default_config})',
    )
    args = ap.parse_args()

    src_params, aliases = parse_config_h(Path(args.config))
    en_params = parse_doc(Path(args.en))
    zh_params = parse_doc(Path(args.zh))
    ja_params = parse_doc(Path(args.ja)) if args.ja else None

    issues: List[str] = []

    en_names = set(en_params)
    zh_names = set(zh_params)
    ja_names = set(ja_params) if ja_params is not None else None

    # 1. Coverage gaps — EN is the reference
    for name in sorted(en_names - zh_names):
        issues.append(f'[MISSING_ZH]   {name}')
    for name in sorted(zh_names - en_names):
        issues.append(f'[MISSING_EN]   {name}  (present in ZH)')

    if ja_names is not None:
        for name in sorted(en_names - ja_names):
            issues.append(f'[MISSING_JA]   {name}')
        for name in sorted(ja_names - en_names):
            issues.append(f'[MISSING_EN]   {name}  (present in JA)')

    # 2. Values vs source
    all_doc_names = en_names | zh_names | (ja_names or set())
    lang_maps = [('EN', en_params), ('ZH', zh_params)]
    if ja_params is not None:
        lang_maps.append(('JA', ja_params))

    for name in sorted(all_doc_names):
        src = src_params.get(name)
        if src is None:
            if name not in aliases:
                issues.append(f'[NOT_IN_SRC]   {name}')
            continue  # aliases: skip value checks

        for lang, doc_map in lang_maps:
            p = doc_map.get(name)
            if p is None:
                continue

            doc_def = normalize_default(p.default)
            src_def = normalize_default(src.default)
            if doc_def is not None and src_def is not None and doc_def != src_def:
                issues.append(
                    f'[DEFAULT_{lang}]  {name}'
                    f'  doc={doc_def!r}  src={src_def!r}'
                )

            if p.mutable is not None and p.mutable != src.mutable:
                issues.append(
                    f'[MUTABLE_{lang}]  {name}'
                    f'  doc={"Yes" if p.mutable else "No"}'
                    f'  src={"Yes" if src.mutable else "No"}'
                )

    doc_files = f'{Path(args.en).name} / {Path(args.zh).name}'
    if args.ja:
        doc_files += f' / {Path(args.ja).name}'

    if issues:
        print(f'Found {len(issues)} issue(s) in {doc_files}:\n')
        for issue in issues:
            print(f'  {issue}')
        return 1

    print(f'All checks passed for {doc_files}.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
