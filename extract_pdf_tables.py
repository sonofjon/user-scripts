"""Extract tables from a PDF file using pdfplumber.

Reads pages, extracts tables with borders, and writes each table
to stdout in CSV format separated by a blank line and a table
header comment.

Usage:
    pip install pdfplumber
    python extract_pdf_tables.py <pdf_file> [-p PAGES] [-m]
                                 [-s SETTINGS] [-o FILE] [-d]

Arguments:
    pdf_file: Path to the PDF file.
    -p, --pages: Page range to extract. Examples:
        17-85  (pages 17 through 85)
        17-    (page 17 to end)
        -85    (start to page 85)
        Omit to extract all pages.
    -m, --merge-tables: Merge tables that are split across page
        boundaries. Only works when all tables in the document
        share the same header row.
    -s, --table-settings: pdfplumber table settings as
        comma-separated key=value pairs.
        Example: -s snap_x_tolerance=5,join_x_tolerance=3
    -o, --output: Output file path. If omitted, writes to stdout.
    -d, --debug: Print one line per raw extracted table to stderr
        at the end of the run, showing raw table number, page, and
        first row. Repeat as -dd to also print grouped first-row
        statistics.

Known limitations:
    Column boundary x-drift: pdfplumber infers column positions
    from vertical line segments whose x-coordinates can vary
    slightly between pages and even within a single table. No
    single snap_x_tolerance value corrects all tables; tuning it
    is a global trade-off with diminishing returns.

    Garbled headers: When x-drift exceeds the snap tolerance,
    pdfplumber splits words across wrong column boundaries (e.g.
    "Typ" and "Storlek" become "Typ S" and "torlek S"). The
    resulting header does not match the canonical form, so
    --merge-tables silently absorbs the affected sub-table into
    its predecessor, losing the boundary between logical tables.

    Text-based column detection (vertical_strategy=text) is not
    a reliable fix: it fragments tables more aggressively due to
    character spacing jitter, and its tuning knobs (text_x_tolerance,
    min_words_vertical) have the same cross-page variance problem.
    In particular, min_words_vertical must be no greater than the
    row count of the smallest table, which constrains how much
    spurious boundary filtering is possible.

    For PDFs with a fixed-width columnar layout, parsing the output
    of pdftotext -layout with column-position rules may be more
    reliable than geometric table extraction.
"""

import argparse
import csv
import sys
from pathlib import Path

import pdfplumber


def parse_table_settings(settings_str):
    """Parse a comma-separated key=value string into a dict.

    Numeric values are converted to int or float as appropriate.

    Args:
        settings_str: Comma-separated key=value pairs, e.g.
            'snap_x_tolerance=5,join_x_tolerance=3'.

    Returns:
        dict: Parsed settings.
    """
    settings = {}
    for pair in settings_str.split(","):
        key, sep, value = pair.partition("=")
        if not sep:
            msg = f"Invalid setting (expected key=value): {pair!r}"
            raise ValueError(msg)
        key = key.strip()
        value = value.strip()
        # Convert numeric values
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                pass
        settings[key] = value
    return settings


def parse_page_range(pages_str, total_pages):
    """Parse a page range string into start and end page numbers.

    Supports formats: '17-85', '17-', '-85'. All values are
    1-based and inclusive.

    Args:
        pages_str: The page range string.
        total_pages: Total number of pages in the PDF.

    Returns:
        tuple: (start_page, end_page) as 1-based integers.
    """
    if "-" not in pages_str:
        # Single page number
        page = int(pages_str)
        if page < 1 or page > total_pages:
            msg = f"Page {page} out of range (1-{total_pages})"
            raise ValueError(msg)
        return page, page

    parts = pages_str.split("-", 1)
    start = int(parts[0]) if parts[0] else 1
    end = int(parts[1]) if parts[1] else total_pages

    if start < 1 or start > total_pages:
        msg = f"Start page {start} out of range (1-{total_pages})"
        raise ValueError(msg)
    if end < 1 or end > total_pages:
        msg = f"End page {end} out of range (1-{total_pages})"
        raise ValueError(msg)
    if start > end:
        msg = f"Start page {start} is after end page {end}"
        raise ValueError(msg)

    return start, end


def clean_cell(cell):
    """Return the cell stripped of whitespace, or empty string."""
    return cell.strip() if cell else ""


def merge_split_tables(tables):
    """Merge tables that are split across page boundaries.

    When a table is split across a page boundary, the continuation
    on the next page does not repeat the header row. This function
    detects continuation tables by comparing each table's first
    row to the previous table's header. A matching first row
    indicates a new table; a non-matching first row indicates a
    continuation, and its rows are appended to the previous table.

    Assumes all tables in the document share the same header row.
    Tables with different headers will be incorrectly merged.

    Args:
        tables: List of table dicts with 'page' and 'rows' keys.

    Returns:
        list: Merged list of table dicts.
    """
    if not tables:
        return tables

    merged = [tables[0]]

    for table in tables[1:]:
        first_row = table["rows"][0]
        prev_header = merged[-1]["rows"][0]
        # Normalize for comparison
        first_row_clean = [clean_cell(cell) for cell in first_row]
        header_clean = [clean_cell(cell) for cell in prev_header]
        if first_row_clean == header_clean:
            # New table (has header): keep as separate table
            merged.append(table)
        else:
            # Continuation: append rows to previous table
            merged[-1]["rows"].extend(table["rows"])

    return merged


def extract_tables(pdf, start_page, end_page, table_settings=None):
    """Extract tables using pdfplumber's ``extract_tables()``.

    Args:
        pdf: An open pdfplumber.PDF object.
        start_page: First page to extract (1-based, inclusive).
        end_page: Last page to extract (1-based, inclusive).
        table_settings: Optional dict of pdfplumber table
            settings passed to ``extract_tables()``.

    Returns:
        list: List of dicts, each with keys 'page' (1-based page
            number) and 'rows' (list of lists of cell values).
    """
    tables = []
    for page_num in range(start_page, end_page + 1):
        page = pdf.pages[page_num - 1]
        page_tables = page.extract_tables(table_settings)
        for table in page_tables:
            # Filter out empty rows
            rows = [
                row
                for row in table
                if any(cell and cell.strip() for cell in row)
            ]
            if rows:
                tables.append(
                    {
                        "page": page_num,
                        "rows": rows,
                    }
                )
    return tables


def report_raw_table_starts(tables, output):
    """Write one debug line per raw extracted table.

    Shows the 1-based raw table index, page number, and first row of
    each table.

    Args:
        tables: List of table dicts from ``extract_tables()``.
        output: File object to write to.
    """
    output.write("[debug] Raw tables:\n")
    for i, table in enumerate(tables):
        first_row = [clean_cell(cell) for cell in table["rows"][0]]
        cells = ", ".join(repr(cell) for cell in first_row)
        output.write(
            f"[debug]   table {i + 1} page {table['page']}: "
            f"{len(first_row)} cols [{cells}]\n"
        )


def report_first_row_variants(tables, output):
    """Write a summary of distinct first rows to a file object.

    Groups tables by first row and writes one line per variant to
    output. The largest group is shown with its count only; smaller
    groups also list the 1-based table indexes and page numbers that
    carry that first row.

    Args:
        tables: List of table dicts from ``extract_tables()``.
        output: File object to write to.
    """
    groups = {}
    for i, table in enumerate(tables):
        first_row = tuple(clean_cell(cell) for cell in table["rows"][0])
        if not first_row:
            continue
        groups.setdefault(first_row, []).append((i + 1, table["page"]))

    # Sort by group size descending; ties broken by first-row tuple
    ordered = sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))

    output.write("[debug] First-row variants:\n")
    for rank, (first_row, indexes) in enumerate(ordered):
        cells = ", ".join(repr(cell) for cell in first_row)
        prefix = f"[debug]   {len(first_row)} cols [{cells}]: "
        if rank == 0:
            output.write(f"{prefix}{len(indexes)} tables\n")
        else:
            joined = ", ".join(
                f"{table_index} (page {page_num})"
                for table_index, page_num in indexes
            )
            output.write(f"{prefix}tables {joined}\n")


def write_tables(tables, output):
    """Write extracted tables to a file object in CSV format.

    Each table is preceded by a comment line with the page number
    and table index, and separated by a blank line.

    Args:
        tables: List of table dicts from ``extract_tables()``.
        output: File object to write to.
    """
    writer = csv.writer(output, lineterminator="\n")
    for i, table in enumerate(tables):
        if i > 0:
            output.write("\n")
        output.write(f"# Table {i + 1} (page {table['page']})\n")
        for row in table["rows"]:
            # Replace None with empty string
            cleaned = [clean_cell(cell) for cell in row]
            writer.writerow(cleaned)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract tables from a PDF file",
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
        "--merge-tables",
        "-m",
        action="store_true",
        help=(
            "Merge tables that are split across page"
            " boundaries. Only works when all tables"
            " share the same header row"
        ),
    )
    parser.add_argument(
        "--table-settings",
        "-s",
        help=(
            "pdfplumber table settings as comma-separated"
            " key=value pairs, e.g."
            " snap_x_tolerance=5,join_x_tolerance=3"
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
        action="count",
        default=0,
        help=(
            "Print raw-table debug output to stderr; repeat as"
            " -dd to also print grouped first-row statistics"
        ),
    )
    args = parser.parse_args()

    # Parse pdfplumber table settings
    table_settings = None
    if args.table_settings:
        table_settings = parse_table_settings(args.table_settings)

    with pdfplumber.open(args.pdf_file) as pdf:
        # Determine page range
        total_pages = len(pdf.pages)

        if args.pages:
            start_page, end_page = parse_page_range(args.pages, total_pages)
        else:
            start_page = 1
            end_page = total_pages

        # Extract tables
        tables = extract_tables(
            pdf,
            start_page,
            end_page,
            table_settings=table_settings,
        )

    # Keep a reference to the raw tables so --debug can report
    # sub-tables that would otherwise be absorbed by the merge
    raw_tables = tables

    # Merge tables that are split across page boundaries
    if args.merge_tables:
        tables = merge_split_tables(tables)

    if not tables:
        print("No tables found.", file=sys.stderr)
        sys.exit(0)

    if args.output:
        with Path(args.output).open(
            "w", newline="", encoding="utf-8",
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

    if args.debug >= 1:
        report_raw_table_starts(raw_tables, sys.stderr)
    if args.debug >= 2:
        report_first_row_variants(raw_tables, sys.stderr)
