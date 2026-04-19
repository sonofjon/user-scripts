"""Extract tables from a PDF file using pdfplumber.

Reads pages, extracts tables with borders, and writes each table
to stdout in CSV format separated by a blank line and a table
header comment.

Usage:
    pip install pdfplumber
    python extract_pdf_tables.py <pdf_file> [-p PAGES] [-t]
                                 [-s SETTINGS] [-o FILE]

Arguments:
    pdf_file: Path to the PDF file.
    -p, --pages: Page range to extract. Examples:
        17-85  (pages 17 through 85)
        17-    (page 17 to end)
        -85    (start to page 85)
        Omit to extract all pages.
    -t, --merge-tables: Merge tables that span multiple pages.
    -s, --table-settings: pdfplumber table settings as
        comma-separated key=value pairs.
        Example: -s snap_x_tolerance=5,join_x_tolerance=3
    -o, --output: Output file path. If omitted, writes to stdout.
"""

import argparse
import csv
import io
import sys

import pdfplumber


def parse_table_settings(settings_str):
    """Parse a comma-separated key=value string into a dict.

    Numeric values are converted to int or float as appropriate.

    Args:
        settings_str: Comma-separated key=value pairs, e.g.
            "snap_x_tolerance=5,join_x_tolerance=3".

    Returns:
        dict: Parsed settings.
    """
    settings = {}
    for pair in settings_str.split(","):
        key, sep, value = pair.partition("=")
        if not sep:
            continue
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

    Supports formats: "17-85", "17-", "-85". All values are
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
        return page, page

    parts = pages_str.split("-", 1)
    start = int(parts[0]) if parts[0] else 1
    end = int(parts[1]) if parts[1] else total_pages
    return start, end


def merge_cross_page_tables(tables):
    """Merge tables that span across page boundaries.

    Detects continuation tables by comparing each table's first
    row to the previous table's header. A table whose first row
    matches the previous table's header is treated as a new
    table. A table whose first row does not match is treated as
    a continuation of the previous table, and its rows are
    appended.

    Limitation: a table with a first row that differs from the
    previous table's header is always treated as a continuation,
    even if it is a genuinely separate table.

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
        first_row_clean = [(cell.strip() if cell else "") for cell in first_row]
        header_clean = [(cell.strip() if cell else "") for cell in prev_header]
        if first_row_clean == header_clean:
            # New table (has header): keep as separate table
            merged.append(table)
        else:
            # Continuation: append rows to previous table
            merged[-1]["rows"].extend(table["rows"])

    return merged


def extract_tables(pdf_path, start_page, end_page, table_settings=None):
    """Extract tables using pdfplumber's extract_tables().

    Args:
        pdf_path: Path to the PDF file.
        start_page: First page to extract (1-based, inclusive).
        end_page: Last page to extract (1-based, inclusive).
        table_settings: Optional dict of pdfplumber table
            settings passed to extract_tables().

    Returns:
        list: List of dicts, each with keys 'page' (1-based page
            number) and 'rows' (list of lists of cell values).
    """
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(start_page, end_page + 1):
            page = pdf.pages[page_num - 1]
            page_tables = page.extract_tables(table_settings)
            for table in page_tables:
                # Filter out empty rows
                rows = [
                    row for row in table if any(cell and cell.strip() for cell in row)
                ]
                if rows:
                    tables.append(
                        {
                            "page": page_num,
                            "rows": rows,
                        }
                    )
    return tables


def write_tables(tables, output):
    """Write extracted tables to a file object in CSV format.

    Each table is preceded by a comment line with the page number
    and table index, and separated by a blank line.

    Args:
        tables: List of table dicts from extract_tables().
        output: File object to write to.
    """
    writer = csv.writer(output)
    for i, table in enumerate(tables):
        if i > 0:
            output.write("\n")
        output.write(f"# Table {i + 1} (page {table['page']})\n")
        for row in table["rows"]:
            # Replace None with empty string
            cleaned = [cell.strip() if cell else "" for cell in row]
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
        help=("Page range: 17-85, 17- (to end), -85 (from start)"),
    )
    parser.add_argument(
        "--merge-tables",
        "-t",
        action="store_true",
        help="Merge tables that span multiple pages",
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
    args = parser.parse_args()

    # Determine page range
    with pdfplumber.open(args.pdf_file) as pdf:
        total_pages = len(pdf.pages)

    if args.pages:
        start_page, end_page = parse_page_range(args.pages, total_pages)
    else:
        start_page = 1
        end_page = total_pages

    # Parse pdfplumber table settings
    table_settings = None
    if args.table_settings:
        table_settings = parse_table_settings(args.table_settings)

    # Extract tables
    tables = extract_tables(
        args.pdf_file,
        start_page,
        end_page,
        table_settings=table_settings,
    )

    # Merge tables that span across page boundaries
    if args.merge_tables:
        tables = merge_cross_page_tables(tables)

    if not tables:
        print("No tables found.", file=sys.stderr)
        sys.exit(1)

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            write_tables(tables, f)
        print(
            f"Extracted {len(tables)} tables to {args.output}",
            file=sys.stderr,
        )
    else:
        output = io.StringIO()
        write_tables(tables, output)
        print(output.getvalue(), end="")
        print(
            f"Extracted {len(tables)} tables",
            file=sys.stderr,
        )
