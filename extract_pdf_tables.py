"""Extract tables from a PDF file using pdfplumber.

Reads pages, extracts tables with borders, and writes each table
to stdout in CSV format separated by a blank line and a table
header comment.

Two extraction methods are available:
- tables (default): Uses pdfplumber's extract_tables().
- geometry: Uses cell bounding box geometry to determine the true
  row structure from the first column's cell boundaries.

Optional post-processing:
- Row merging (-r): When pdfplumber splits multi-line cell text
  into separate rows (detected by empty first cell), this option
  merges them back into the previous row.
- Table merging (-t): Merges tables that span across page
  boundaries by detecting continuation tables without a header.

Usage:
    pip install pdfplumber
    python extract_pdf_tables.py <pdf_file> [-p PAGES] [-m METHOD]
                                 [-r] [-t] [-s SETTINGS]
                                 [-o FILE]

Arguments:
    pdf_file: Path to the PDF file.
    -p, --pages: Page range to extract. Examples:
        17-85  (pages 17 through 85)
        17-    (page 17 to end)
        -85    (start to page 85)
        Omit to extract all pages.
    -m, --method: Extraction method: "tables" (default) or
        "geometry".
    -r, --merge-rows: Merge multi-line cell rows.
    -t, --merge-tables: Merge cross-page tables.
    -s, --table-settings: pdfplumber table settings as
        comma-separated key=value pairs. Example:
        -s snap_x_tolerance=5,join_x_tolerance=3
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


def merge_continuation_rows(rows):
    """Merge rows where the first cell is empty into the previous row.

    When pdfplumber splits multi-line cell text into separate rows,
    the continuation rows have an empty first cell. This function
    appends the non-empty cell values from continuation rows to
    the corresponding cells of the previous row, separated by a
    newline.

    Args:
        rows: List of rows (each a list of cell values).

    Returns:
        list: Merged rows.
    """
    if not rows:
        return rows

    merged = [rows[0]]
    for row in rows[1:]:
        first_cell = row[0].strip() if row[0] else ""
        if not first_cell and merged:
            # Continuation row: merge into previous
            prev = merged[-1]
            for i, cell in enumerate(row):
                if i < len(prev) and cell and cell.strip():
                    prev_val = prev[i].strip() if prev[i] else ""
                    if prev_val:
                        prev[i] = prev_val + "\n" + cell.strip()
                    else:
                        prev[i] = cell.strip()
        else:
            merged.append(row)
    return merged


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
        first_row_clean = [
            (cell.strip() if cell else "")
            for cell in first_row
        ]
        header_clean = [
            (cell.strip() if cell else "")
            for cell in prev_header
        ]
        if first_row_clean == header_clean:
            # New table (has header): keep as separate table
            merged.append(table)
        else:
            # Continuation: append rows to previous table
            merged[-1]["rows"].extend(table["rows"])

    return merged


def build_rows_from_cells(page, table):
    """Extract table rows using cell bounding box geometry.

    Uses the first column's cell boundaries to determine the true
    row structure. Each first-column cell spans the full height of
    its logical row. All cells whose y-range falls within a
    first-column cell's y-range are grouped into that row, and
    multi-line text within a cell is joined with a space.

    Args:
        page: A pdfplumber Page object.
        table: A pdfplumber Table object.

    Returns:
        list: List of rows, each a list of cell text strings.
    """
    cells = table.cells
    if not cells:
        return []

    # Find unique x boundaries (column edges) sorted left to right
    x_edges = sorted(set(
        x for cell in cells for x in (cell[0], cell[2])
    ))

    # Find unique y boundaries for the first column to
    # determine logical rows
    first_col_x0 = x_edges[0]
    first_col_x1 = x_edges[1] if len(x_edges) > 1 else None

    # Collect first-column cells (their y-ranges define rows)
    row_boundaries = []
    for cell in cells:
        x0, y0, x1, y1 = cell
        if (abs(x0 - first_col_x0) < 1
                and (first_col_x1 is None
                     or abs(x1 - first_col_x1) < 1)):
            row_boundaries.append((y0, y1))

    # Sort by y position (top to bottom)
    row_boundaries.sort(key=lambda r: r[0])

    # Determine column positions from x_edges
    col_ranges = list(zip(x_edges[:-1], x_edges[1:]))

    # Build rows: for each logical row, collect text from all
    # cells that fall within its y-range, grouped by column
    rows = []
    for row_y0, row_y1 in row_boundaries:
        row = []
        for col_x0, col_x1 in col_ranges:
            # Find all cells in this row/column intersection
            cell_texts = []
            for cell in cells:
                cx0, cy0, cx1, cy1 = cell
                # Cell belongs to this column if it overlaps
                # horizontally
                if cx0 >= col_x0 - 1 and cx1 <= col_x1 + 1:
                    # Cell belongs to this row if it overlaps
                    # vertically
                    if cy0 >= row_y0 - 1 and cy1 <= row_y1 + 1:
                        crop = page.within_bbox(cell)
                        text = crop.extract_text() or ""
                        if text.strip():
                            cell_texts.append(text.strip())
            row.append(" ".join(cell_texts))
        rows.append(row)

    return rows


def extract_tables_default(pdf_path, start_page, end_page,
                           merge_rows=False, table_settings=None):
    """Extract tables using pdfplumber's extract_tables().

    Optionally post-processes with merge_continuation_rows() to
    handle multi-line cells.

    Args:
        pdf_path: Path to the PDF file.
        start_page: First page to extract (1-based, inclusive).
        end_page: Last page to extract (1-based, inclusive).
        merge_rows: If True, merge continuation rows caused by
            pdfplumber splitting multi-line cells.
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
                    row for row in table
                    if any(
                        cell and cell.strip()
                        for cell in row
                    )
                ]
                if rows:
                    if merge_rows:
                        # Merge continuation rows caused by
                        # pdfplumber splitting multi-line cells
                        rows = merge_continuation_rows(rows)
                    tables.append({
                        "page": page_num,
                        "rows": rows,
                    })
    return tables


def extract_tables_geometry(pdf_path, start_page, end_page,
                            table_settings=None):
    """Extract tables using cell geometry approach.

    Uses cell bounding boxes to determine the true row structure,
    avoiding false row splits from multi-line cells.

    Args:
        pdf_path: Path to the PDF file.
        start_page: First page to extract (1-based, inclusive).
        end_page: Last page to extract (1-based, inclusive).
        table_settings: Optional dict of pdfplumber table
            settings passed to find_tables().

    Returns:
        list: List of dicts, each with keys 'page' (1-based page
            number) and 'rows' (list of lists of cell values).
    """
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num in range(start_page, end_page + 1):
            page = pdf.pages[page_num - 1]
            page_tables = page.find_tables(table_settings)
            for table in page_tables:
                rows = build_rows_from_cells(page, table)
                # Filter out empty rows
                rows = [
                    row for row in rows
                    if any(cell.strip() for cell in row)
                ]
                if rows:
                    tables.append({
                        "page": page_num,
                        "rows": rows,
                    })
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
        output.write(
            f"# Table {i + 1} (page {table['page']})\n"
        )
        for row in table["rows"]:
            # Replace None with empty string
            cleaned = [
                cell.strip() if cell else ""
                for cell in row
            ]
            writer.writerow(cleaned)


def main():
    """Parse arguments and run table extraction."""
    parser = argparse.ArgumentParser(
        description="Extract tables from a PDF file",
    )
    parser.add_argument(
        "pdf_file",
        help="Path to the PDF file",
    )
    parser.add_argument(
        "--pages", "-p",
        help=(
            "Page range: 17-85, 17- (to end),"
            " -85 (from start)"
        ),
    )
    parser.add_argument(
        "--method", "-m",
        choices=["tables", "geometry"],
        default="tables",
        help=(
            "Extraction method: tables (default) or geometry"
        ),
    )
    parser.add_argument(
        "--merge-rows", "-r",
        action="store_true",
        help="Merge multi-line cell rows",
    )
    parser.add_argument(
        "--merge-tables", "-t",
        action="store_true",
        help="Merge cross-page tables",
    )
    parser.add_argument(
        "--table-settings", "-s",
        help=(
            "pdfplumber table settings as comma-separated"
            " key=value pairs, e.g."
            " snap_x_tolerance=5,join_x_tolerance=3"
        ),
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: stdout)",
    )
    args = parser.parse_args()

    # Determine page range
    with pdfplumber.open(args.pdf_file) as pdf:
        total_pages = len(pdf.pages)

    if args.pages:
        start_page, end_page = parse_page_range(
            args.pages, total_pages
        )
    else:
        start_page = 1
        end_page = total_pages

    # Parse pdfplumber table settings
    table_settings = None
    if args.table_settings:
        table_settings = parse_table_settings(
            args.table_settings
        )

    # Extract tables using selected method
    merge_rows = args.merge_rows
    if args.method == "geometry":
        tables = extract_tables_geometry(
            args.pdf_file, start_page, end_page,
            table_settings=table_settings,
        )
    elif args.method == "tables":
        tables = extract_tables_default(
            args.pdf_file, start_page, end_page,
            merge_rows=merge_rows,
            table_settings=table_settings,
        )

    # Merge tables that span across page boundaries
    if args.merge_tables:
        tables = merge_cross_page_tables(tables)

    if not tables:
        print("No tables found.", file=sys.stderr)
        sys.exit(1)

    if args.output:
        with open(args.output, "w", newline="",
                  encoding="utf-8") as f:
            write_tables(tables, f)
        print(
            f"Extracted {len(tables)} tables to"
            f" {args.output}",
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


if __name__ == "__main__":
    main()
