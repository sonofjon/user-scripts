"""Extract tables from a PDF via ``pdftotext -layout`` parsing.

Runs ``pdftotext -layout`` on the PDF, then parses tables by locating header
lines and slicing subsequent data rows at header-derived character-column
positions. All tables in the PDF must have the same column names and order,
as a single --header-pattern identifies all table headers.

pdftotext -layout sometimes emits blank lines within a table (e.g. when a
row has an empty second line). Since a blank line normally ends the current
table, these would cause the remaining rows to be silently dropped. To avoid
this, blank lines are not treated as table terminators if the next non-blank
line looks like a table row, as determined by a gaps test and a whitespace
ratio test (see --row-gaps and --row-ratio).

Usage:
    Requires pdftotext on PATH (poppler-utils).

    python extract_pdf_text_tables.py <pdf_file>
        --header-pattern COL1,COL2,...
        [-p PAGES] [--end-pattern REGEX]
        [-m] [--page-break-pattern REGEX]
        [-o FILE] [-d]
        [--row-gaps N M] [--no-row-gaps]
        [--row-ratio R] [--no-row-ratio]

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
    --row-gaps N M: Minimum number of inter-word gaps and minimum gap
        length (in spaces) for gaps-based table row detection. A
        non-blank line after a blank is kept in the table if it contains
        at least N runs of M or more consecutive spaces between
        non-whitespace tokens (leading and trailing whitespace ignored).
        Use --no-row-gaps to disable. (default: 3 3)
    --no-row-gaps: Disable the gaps-based table row shape test.
    --row-ratio R: Minimum interior whitespace ratio for ratio-based
        table row detection. A non-blank line after a blank is kept in
        the table if the fraction of its interior characters (leading and
        trailing whitespace stripped) that are spaces is at least R.
        Use --no-row-ratio to disable. (default: 0.30)
    --no-row-ratio: Disable the ratio-based table row shape test.

Known limitations:
    1. When --merge-tables is used, column positions for continuation rows
       are inferred from the first header found on the continuation page,
       which belongs to a different table. Since pdftotext may render
       different tables at different character positions, the inferred
       positions may not match the continuation rows, causing values to be
       cut at wrong column boundaries and possibly end up in the wrong
       cells, wholly or partially.
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
            cmd,
            capture_output=True,
            text=True,
            check=True,
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


def _looks_like_table_row(line, row_gaps, row_ratio):
    """Return True if line looks like a table data row by shape.

    Uses two independent shape tests, either of which is sufficient
    (OR logic). Leading and trailing whitespace are ignored in both
    tests. If both row_gaps and row_ratio are None, returns False so
    that any blank line ends the table.

    Args:
        line: The line to check.
        row_gaps: Tuple (min_gaps, min_gap_len) for the gaps test, or
            None to skip the test. A line passes if it contains at
            least min_gaps runs of min_gap_len or more consecutive
            spaces between non-whitespace tokens.
        row_ratio: Minimum interior whitespace ratio for the ratio
            test, or None to skip the test. A line passes if the
            fraction of its interior characters that are spaces is at
            least this value.

    Returns:
        bool: True if the line looks like a table row.
    """
    if not line.strip():
        return False
    interior = line.strip()
    if row_gaps is not None:
        min_gaps, min_gap_len = row_gaps
        gaps = [
            len(run)
            for run in re.findall(r" +", interior)
            if len(run) >= min_gap_len
        ]
        if len(gaps) >= min_gaps:
            return True
    if row_ratio is not None:
        n_spaces = sum(1 for c in interior if c == " ")
        if n_spaces / len(interior) >= row_ratio:
            return True
    return False


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
    row_gaps=(3, 3),
    row_ratio=0.30,
    debug=False,
):
    """Parse tables from pdftotext layout output.

    Scans the text line by line. A match against ``header_pattern`` starts a
    new table and defines its column starts. Subsequent lines become data
    rows, sliced at those column starts. A table ends at the next header
    match, at a line matching ``end_pattern``, or (when ``merge_tables`` is
    False) at a page break. When a blank line is encountered inside a table,
    it is buffered and the next non-blank line is tested with
    ``_looks_like_table_row``. If the test passes the blank is skipped and
    the table continues; otherwise the blank closes the table.

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
        row_gaps: Tuple (min_gaps, min_gap_len) for the gaps
            shape test, or None to disable it.
        row_ratio: Minimum interior whitespace ratio for the
            ratio shape test, or None to disable it.
        debug: If True, prints header matches and derived
            column positions to stderr.

    Returns:
        tuple: (tables, unmerged_count) where tables is a list of
            table dicts with keys 'page' and 'rows', and
            unmerged_count is the number of tables that could not
            be merged due to missing header on continuation page.
    """
    tables = []
    unmerged_count = 0
    current_table = None
    current_cols = None
    buffered_blank = False
    # pdftotext emits \n before each \f, so splitting on "\n\f"
    # avoids a spurious trailing blank line per page that would
    # falsely close an open table. If a PDF ever omits the \n
    # before \f, use text.split("\f") instead.
    pages = text.split("\n\f")

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
                buffered_blank = False
            skipping_page_break = merge_tables
            if merge_tables and current_table is not None:
                # Column positions can change between pages. Pre-scan
                # this continuation page for its first header and use
                # those positions for all data on this page, including
                # the continuation rows that precede the header.
                header_found = False
                for scan_line in lines:
                    scan_match = header_pattern.search(scan_line)
                    if scan_match:
                        current_cols = find_columns(scan_match)
                        header_found = True
                        if debug:
                            print(
                                f"[debug] pre-scan updated columns on"
                                f" page {page_num} to"
                                f" {current_cols}",
                                file=sys.stderr,
                            )
                        break
                if not header_found:
                    # Cannot determine column positions for the
                    # continuation: output the table as-is without
                    # merging.
                    tables.append(current_table)
                    current_table = None
                    current_cols = None
                    buffered_blank = False
                    unmerged_count += 1
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
                buffered_blank = False
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
                buffered_blank = False
                continue

            if is_blank_line(line):
                buffered_blank = True
                continue

            if end_pattern is not None and end_pattern.search(line):
                tables.append(current_table)
                current_table = None
                current_cols = None
                buffered_blank = False
                continue

            if buffered_blank:
                buffered_blank = False
                if not _looks_like_table_row(line, row_gaps, row_ratio):
                    # The blank ended the table; this line is non-table
                    # content and is skipped.
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

    return tables, unmerged_count


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
    parser.add_argument(
        "--row-gaps",
        nargs=2,
        type=int,
        metavar=("N", "M"),
        default=[3, 3],
        help=(
            "Gaps shape test: keep table open after a blank if the"
            " next line has at least N inter-word gaps of M or more"
            " spaces (default: 3 3)"
        ),
    )
    parser.add_argument(
        "--no-row-gaps",
        action="store_true",
        help="Disable the gaps-based table row shape test",
    )
    parser.add_argument(
        "--row-ratio",
        type=float,
        metavar="R",
        default=0.30,
        help=(
            "Ratio shape test: keep table open after a blank if the"
            " next line's interior whitespace fraction is at least R"
            " (default: 0.30)"
        ),
    )
    parser.add_argument(
        "--no-row-ratio",
        action="store_true",
        help="Disable the ratio-based table row shape test",
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

    row_gaps = None if args.no_row_gaps else tuple(args.row_gaps)
    row_ratio = None if args.no_row_ratio else args.row_ratio

    text = run_pdftotext(args.pdf_file, start_page, end_page)
    tables, unmerged_count = parse_tables(
        text,
        header_pattern=header_pattern,
        end_pattern=end_pattern,
        merge_tables=args.merge_tables,
        page_break_pattern=page_break_pattern,
        start_page=start_page,
        row_gaps=row_gaps,
        row_ratio=row_ratio,
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
    if unmerged_count:
        print(
            f"Warning: {unmerged_count} table(s) could not be merged"
            " (no header found on continuation page).",
            file=sys.stderr,
        )
