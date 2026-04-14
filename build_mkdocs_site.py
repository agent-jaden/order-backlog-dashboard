from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = ROOT / "outputs" / "수주잔고"
DOCS_DIR = ROOT / "docs"
COMPANIES_DIR = DOCS_DIR / "companies"
def main() -> None:
    if not OUTPUTS_DIR.exists():
        raise SystemExit(f"Missing source directory: {OUTPUTS_DIR}")

    _reset_docs_dir()
    _write_index_page()
    _export_dashboard()
    _export_company_pages()
    _write_companies_index()
    print(f"MkDocs docs exported to: {DOCS_DIR}")


def _reset_docs_dir() -> None:
    COMPANIES_DIR.mkdir(parents=True, exist_ok=True)


def _write_index_page() -> None:
    content = "\n".join(
        [
            "# 수주잔고 리포트",
            "",
            "- [대시보드](dashboard.md)",
            "- [기업별 문서 목록](companies/index.md)",
            "",
            "이 사이트는 DART 기반 수주잔고 시계열과 대시보드를 게시하기 위한 정적 문서 사이트입니다.",
            "",
        ]
    )
    (DOCS_DIR / "index.md").write_text(content, encoding="utf-8")


def _export_dashboard() -> None:
    source = OUTPUTS_DIR / "수주잔고_대시보드.md"
    target = DOCS_DIR / "dashboard.md"
    text = source.read_text(encoding="utf-8-sig")
    text = _replace_local_company_links(text, prefix="companies/")
    target.write_text(text, encoding="utf-8")


def _export_company_pages() -> None:
    for source in sorted(OUTPUTS_DIR.glob("*_수주잔고(*).md")):
        target = COMPANIES_DIR / source.name
        text = source.read_text(encoding="utf-8-sig")
        text = _replace_local_company_links(text, prefix="")
        target.write_text(text, encoding="utf-8")


def _write_companies_index() -> None:
    company_files = sorted(
        file_path
        for file_path in COMPANIES_DIR.glob("*.md")
        if not file_path.name.startswith("_수주잔고(")
    )
    lines = [
        "# 기업별 문서 목록",
        "",
        f"- 문서 수: `{len(company_files)}`",
        "",
        "| 기업 문서 |",
        "| --- |",
    ]
    for file_path in company_files:
        lines.append(f"| [{file_path.stem}]({file_path.name}) |")
    lines.append("")
    (COMPANIES_DIR / "index.md").write_text("\n".join(lines), encoding="utf-8")


def _replace_local_company_links(text: str, prefix: str) -> str:
    href_pattern = re.compile(r'href="([^"]+_수주잔고\(\d+\)\.md)"')
    text = href_pattern.sub(lambda match: f'href="{prefix}{Path(match.group(1)).stem}/"', text)

    markdown_pattern = re.compile(r"\((?:\./)?([^)\r\n]+_수주잔고\(\d+\)\.md)\)")
    text = markdown_pattern.sub(lambda match: f"({prefix}{match.group(1)})", text)

    for source in OUTPUTS_DIR.glob("*_수주잔고(*).md"):
        relative_md = f"{prefix}{source.name}"
        relative_page = f"{prefix}{source.stem}/"
        pattern = re.compile(rf"\([A-Za-z]:\\[^)\r\n]*\\{re.escape(source.name)}\)")
        text = pattern.sub(f"({relative_md})", text)
        text = text.replace(f'href="{source.name}"', f'href="{relative_page}"')
        text = text.replace(f"({source.name})", f"({relative_md})")
        text = text.replace(f"(./{source.name})", f"({relative_md})")
    return text


if __name__ == "__main__":
    main()
