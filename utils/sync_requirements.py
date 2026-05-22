"""
sync_requirements.py — Regenerate requirements.txt from all tracked (non-gitignored) .py files.

Asks git for the .py files a cloner actually receives (`git ls-files '*.py'`),
AST-scans their imports, drops stdlib + local modules, maps the rest to installed
pip distributions, and pins to the installed versions.

Usage:
    python utils/sync_requirements.py
"""
import ast
import subprocess
import sys
from collections import defaultdict
from importlib.metadata import packages_distributions, version, PackageNotFoundError
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Runtime-only deps (not imported in code, but required to run something).
EXTRA_PACKAGES = ["uvicorn"]


def tracked_py_files() -> list[Path]:
    """The .py files a cloner receives: committed and not gitignored, at any depth."""
    out = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout
    return [REPO_ROOT / line for line in out.splitlines() if line]


def top_level_imports(py_file: Path) -> set[str]:
    """Extract top-level module names from import statements in one file."""
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return set()
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
    return names


def local_module_names(py_files: list[Path]) -> set[str]:
    """Top-level names that resolve to local code, so they're never mistaken for deps.

    Covers three cases:
      - root-level modules (`foo.py` -> `import foo`)
      - top-level package/dirs (`from scraper.x import ...` -> `scraper`)
      - bare-import source roots: a dir whose files import *siblings* by bare name
        (e.g. scripts/ does `from edgar import ...` and scripts/edgar.py exists, so
        the local `edgar` isn't confused with the PyPI `edgar` package).

    Assumes no local file is named after a third-party package (don't create an
    openai.py); such a file would shadow the package and drop it from requirements.
    """
    dir_stems: dict[str, set[str]] = defaultdict(set)  # "" key = repo root
    for p in py_files:
        rel = p.relative_to(REPO_ROOT)
        dir_stems["/".join(rel.parts[:-1])].add(rel.stem)

    local = set(dir_stems.get("", set()))                       # root-level modules
    local |= {p.relative_to(REPO_ROOT).parts[0]                 # top-level dir/package names
              for p in py_files if len(p.relative_to(REPO_ROOT).parts) > 1}

    for p in py_files:                                          # bare-import source roots
        d = "/".join(p.relative_to(REPO_ROOT).parts[:-1])
        if d and dir_stems[d] & top_level_imports(p):
            local |= dir_stems[d]
    return local


def main():
    py_files = tracked_py_files()
    local_modules = local_module_names(py_files)

    stdlib = sys.stdlib_module_names
    mod_to_dist = packages_distributions()  # {"cv2": ["opencv-python"], "yaml": ["PyYAML"], ...}

    imports: set[str] = set()
    for py in py_files:
        imports |= top_level_imports(py)

    pkgs: dict[str, str] = {}
    unresolved: list[str] = []

    for extra in EXTRA_PACKAGES:
        try:
            pkgs[extra.lower()] = f"{extra}=={version(extra)}"
        except PackageNotFoundError:
            unresolved.append(extra)

    for mod in sorted(imports):
        if mod in stdlib or mod in local_modules or mod.startswith("_"):
            continue
        dists = mod_to_dist.get(mod)
        if not dists:
            unresolved.append(mod)
            continue
        for dist in dists:
            try:
                pkgs[dist.lower()] = f"{dist}=={version(dist)}"
            except PackageNotFoundError:
                unresolved.append(dist)

    out_path = REPO_ROOT / "requirements.txt"
    out_path.write_text("\n".join(sorted(pkgs.values(), key=str.lower)) + "\n", encoding="utf-8")
    print(f"Wrote {len(pkgs)} packages from {len(py_files)} tracked .py files -> {out_path}")
    if unresolved:
        print(f"Unresolved imports (likely local or not installed): {sorted(set(unresolved))}")


if __name__ == "__main__":
    main()
