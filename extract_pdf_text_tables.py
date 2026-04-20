"""Extract tables from a PDF via ``pdftotext -layout`` parsing.

Runs ``pdftotext -layout`` on the PDF, then parses tables by locating header
lines and slicing subsequent data rows at header-derived character-column
positions.

Usage:
    Requires pdftotext on PATH (poppler-utils).

    python extract_pdf_text_tables.py <pdf_file>
        --header-pattern COL1,COL2,...
        [-p PAGES] [--end-pattern REGEX]
        [-m] [--page-break-pattern REGEX]
        [-o FILE] [-d]

Arguments:
    pdf_file: Path to the PDF file.
    --header-pattern: Comma-separated list of column heading
        patterns. Each entry is a regex or literal matched
        against one column's heading in the header line. The
        entries are matched in order; each match's start
        position defines a column start. Multi-word column
        headings are supported (e.g.
        'First Name,Last Name,Age'). Use '\\,' to include a
        literal comma in an entry.
    -p, --pages: Page range. Examples:
        17-85  (pages 17 through 85)
        17-    (page 17 to end)
        -85    (start to page 85)
        Omit to extract all pages.
    --end-pattern: Optional regex. A line matching this
        regex ends the current table.
    -m, --merge-tables: Merge tables that are split across page
        boundaries. Lines matching --page-break-pattern are
        skipped, then the previous page's column positions are
        reused on the continuation page.
    --page-break-pattern: Regex matching lines to skip after a
        page break (e.g. page numbers, running headers). Only
        used when --merge-tables is set.
    -o, --output: Output file path. If omitted, writes to
        stdout.
    -d, --debug: Print header matches and derived column
        positions to stderr.
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path


def parse_page_range(pages_str):
    """Parse a page range string into start and end page numbers.

    Supports formats: '17-85', '17-', '-85', '17'. Values are 1-based and
    inclusive.

    Args:
        pages_str: The page range string.

    Returns:
        tuple: (start_page, end_page) as 1-based ints. end_page
            is None when the range is open-ended (e.g. '17-').
    """
    if "-" not in pages_str:
        page = int(pages_str)
        return page, page
    parts = pages_str.split("-", 1)
    start = int(parts[0]) if parts[0] else 1
    end = int(parts[1]) if parts[1] else None
    if start < 1:
        msg = f"Start page {start} is less than 1"
        raise ValueError(msg)
    if end is not None and end < start:
        msg = f"End page {end} is before start page {start}"
        raise ValueError(msg)
    return start, end


def run_pdftotext(pdf_file, start_page, end_page):
    """Run ``pdftotext -layout`` and return the stdout text.

    Args:
        pdf_file: Path to the PDF file.
        start_page: First page (1-based, inclusive), or None.
        end_page: Last page (1-based, inclusive), or None.

    Returns:
        str: Extracted text. Page breaks are marked by '\\f'.
    """
    cmd = ["pdftotext", "-layout"]
    if start_page is not None:
        cmd.extend(["-f", str(start_page)])
    if end_page is not None:
        cmd.extend(["-l", str(end_page)])
    cmd.extend([pdf_file, "-"])
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True
        )
    except subprocess.CalledProcessError as e:
        msg = f"pdftotext failed (exit {e.returncode})"
        if e.stderr:
            msg += f":\n{e.stderr.strip()}"
        raise RuntimeError(msg) from e
    return result.stdout


def split_header_pattern(pattern):
    """Parse a --header-pattern value into a list of per-column regexes.

    Unescaped commas separate entries. A backslash before a comma ('\\,')
    escapes the comma, producing a literal comma inside the entry.

    Args:
        pattern: The raw --header-pattern argument value.

    Returns:
        list: List of per-column regex strings, in order.
    """
    entries = []
    current = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern) and pattern[i + 1] == ",":
            current.append(",")
            i += 2
            continue
        if ch == ",":
            entries.append("".join(current))
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    entries.append("".join(current))
    if any(not entry for entry in entries):
        msg = "Empty column entry in --header-pattern"
        raise ValueError(msg)
    return entries


def build_header_regex(column_patterns):
    """Compile a regex to match a header line and to record per-column starts.

    The regex has the form '(pat1)\\s*(pat2)\\s*(pat3)'. A match against
    this pattern identifies the header line, and each parenthesized piece's
    start position gives the corresponding column heading's starting
    character in the line.

    Args:
        column_patterns: List of per-column regex strings.

    Returns:
        re.Pattern: Compiled regex.
    """
    grouped = [f"({p})" for p in column_patterns]
    return re.compile(r"\s+".join(grouped))


def find_columns(match):
    """Return column start positions from a header-line match.

    Args:
        match: re.Match from the compiled header regex built by
            ``build_header_regex()``.

    Returns:
        list: Column start positions (character indexes).
    """
    return [match.span(i + 1)[0] for i in range(len(match.groups()))]


def slice_row(line, col_starts):
    """Slice a line at col_starts and return stripped cells.

    Each cell spans from its column start to the next column start; the last
    cell runs to end of line.

    Args:
        line: The line to slice.
        col_starts: List of column start positions.

    Returns:
        list: List of cell strings, length equal to ``col_starts``.
    """
    cells = []
    for i, start in enumerate(col_starts):
        if i + 1 < len(col_starts):
            cell = line[start : col_starts[i + 1]]
        else:
            cell = line[start:]
        cells.append(cell.strip())
    return cells


def is_continuation_row(cells):
    """Return True if cells look like a multi-line continuation.

    A continuation row has an empty first cell and at least one non-empty
    later cell, indicating wrapped text from a cell in the previous data
    row.

    Args:
        cells: List of cell strings from ``slice_row()``.

    Returns:
        bool: True if cells look like a continuation row.
    """
    if not cells:
        return False
    return not cells[0] and any(cell for cell in cells[1:])


def is_blank_line(line):
    """Return True if line is empty or whitespace-only."""
    return not line.strip()


def merge_continuation(prev_row, cells):
    """Append non-empty cells onto the previous row's cells.

    Joins each appended cell to the existing cell with a newline. Empty
    cells in ``cells`` are ignored.

    Args:
        prev_row: The previous data row (list of cells), mutated
            in place.
        cells: The continuation row's cells.
    """
    for i, cell in enumerate(cells):
        if not cell:
            continue
        if i >= len(prev_row):
            prev_row.extend([""] * (i - len(prev_row) + 1))
        if prev_row[i]:
            prev_row[i] = f"{prev_row[i]}\n{cell}"
        else:
            prev_row[i] = cell


def parse_tables(
    text,
    header_pattern,
    end_pattern=None,
    merge_tables=False,
    page_break_pattern=None,
    start_page=1,
    debug=False,
):
    """Parse tables from pdftotext layout output.

    Scans the text line by line. A match against ``header_pattern`` starts a
    new table and defines its column starts. Subsequent lines become data
    rows, sliced at those column starts. A table ends at a blank line, at the
    next header match, at a line matching ``end_pattern``, or (when
    ``merge_tables`` is False) at a page break.

    Args:
        text: Output of pdftotext -layout.
        header_pattern: Compiled regex. A ``search`` match marks
            a header line.
        end_pattern: Optional compiled regex ending the table.
        merge_tables: If True, tables split across page
            boundaries are merged after skipping lines matched
            by ``page_break_pattern``.
        page_break_pattern: Optional compiled regex; lines
            matching it are skipped after a page break. Only
            applied when ``merge_tables`` is True.
        start_page: 1-based page number of the first page.
        debug: If True, prints header matches and derived
            column positions to stderr.

    Returns:
        list: List of table dicts with keys 'page' and 'rows'.
    """
    tables = []
    current_table = None
    current_cols = None
    pages = text.split("\f")

    for page_offset, page_text in enumerate(pages):
        page_num = start_page + page_offset
        lines = page_text.split("\n")
        # On a continuation page, end the current table when
        # merging is disabled. When merging, skip lines matching
        # page_break_pattern until the first line that looks like
        # data.
        if page_offset > 0:
            if not merge_tables and current_table is not None:
                tables.append(current_table)
                current_table = None
                current_cols = None
            skipping_page_break = merge_tables
        else:
            skipping_page_break = False

        for line in lines:
            if skipping_page_break:
                if is_blank_line(line):
                    continue
                if (
                    page_break_pattern is not None
                    and page_break_pattern.search(line)
                ):
                    continue
                skipping_page_break = False

            match = header_pattern.search(line)
            if match:
                if current_table is not None:
                    tables.append(current_table)
                col_starts = find_columns(match)
                if debug:
                    print(
                        f"[debug] header on page {page_num} at"
                        f" columns {col_starts}: {line.rstrip()!r}",
                        file=sys.stderr,
                    )
                header_cells = slice_row(line, col_starts)
                current_table = {
                    "page": page_num,
                    "rows": [header_cells],
                }
                current_cols = col_starts
                continue

            if current_table is None:
                continue

            if is_blank_line(line):
                tables.append(current_table)
                current_table = None
                current_cols = None
                continue

            if end_pattern is not None and end_pattern.search(line):
                tables.append(current_table)
                current_table = None
                current_cols = None
                continue

            cells = slice_row(line, current_cols)
            if is_continuation_row(cells) and len(current_table["rows"]) > 1:
                merge_continuation(current_table["rows"][-1], cells)
            else:
                current_table["rows"].append(cells)

    if current_table is not None:
        tables.append(current_table)

    return tables


def write_tables(tables, output):
    """Write extracted tables to a file object in CSV format.

    Each table is preceded by a comment line with the page number and table
    index, and separated by a blank line.

    Args:
        tables: List of table dicts from ``parse_tables()``.
        output: File object to write to.
    """
    writer = csv.writer(output, lineterminator="\n")
    for i, table in enumerate(tables):
        if i > 0:
            output.write("\n")
        output.write(f"# Table {i + 1} (page {table['page']})\n")
        for row in table["rows"]:
            writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract tables from a PDF via pdftotext",
    )
    parser.add_argument(
        "pdf_file",
        help="Path to the PDF file",
    )
    parser.add_argument(
        "--pages",
        "-p",
        help="Page range: 17-85, 17- (to end), -85 (from start)",
    )
    parser.add_argument(
        "--header-pattern",
        required=True,
        metavar="COL1,COL2,...",
        help=(
            "Comma-separated list of column heading regexes"
            " (one per column). Use '\\,' to embed a literal"
            " comma. Multi-word headings are supported"
        ),
    )
    parser.add_argument(
        "--end-pattern",
        help="Regex; a line matching it ends the current table",
    )
    parser.add_argument(
        "--merge-tables",
        "-m",
        action="store_true",
        help=(
            "Merge tables that are split across page"
            " boundaries. Lines matching --page-break-pattern"
            " are skipped, then the previous page's column"
            " positions are reused on the continuation page"
        ),
    )
    parser.add_argument(
        "--page-break-pattern",
        help=(
            "Regex; matching lines after a page break are"
            " skipped. Requires --merge-tables"
        ),
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Print header matches and derived column positions to stderr",
    )
    args = parser.parse_args()

    column_patterns = split_header_pattern(args.header_pattern)
    header_pattern = build_header_regex(column_patterns)

    end_pattern = re.compile(args.end_pattern) if args.end_pattern else None
    page_break_pattern = (
        re.compile(args.page_break_pattern)
        if args.page_break_pattern
        else None
    )

    if args.pages:
        start_page, end_page = parse_page_range(args.pages)
    else:
        start_page, end_page = 1, None

    text = run_pdftotext(args.pdf_file, start_page, end_page)
    tables = parse_tables(
        text,
        header_pattern=header_pattern,
        end_pattern=end_pattern,
        merge_tables=args.merge_tables,
        page_break_pattern=page_break_pattern,
        start_page=start_page,
        debug=args.debug,
    )

    if not tables:
        print("No tables found.", file=sys.stderr)
        sys.exit(0)

    if args.output:
        with Path(args.output).open(
            "w",
            newline="",
            encoding="utf-8",
        ) as f:
            write_tables(tables, f)
        print(
            f"Extracted {len(tables)} table(s) to {args.output}",
            file=sys.stderr,
        )
    else:
        write_tables(tables, sys.stdout)
        print(
            f"Extracted {len(tables)} table(s)",
            file=sys.stderr,
        )
