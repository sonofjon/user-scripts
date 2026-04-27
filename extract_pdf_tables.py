"""Extract tables from a PDF file using pdfplumber.

Reads pages, extracts tables, and writes each table to stdout in CSV format
separated by a blank line and a comment line. Requires that all tables in
the document share the same header row.

A table in the PDF may span multiple pages, in which case only the first
page carries the header row; subsequent pages (continuation pages) contain
data rows without repeating the header. The script handles this by ensuring
that all pages of a split table use the same column layout, derived from the
header row.

If pdfplumber infers the wrong cell boundaries for a table region, the
script can locally rebuild rows from the positions of words extracted inside
the table's bounding box.

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
        boundaries.
    -s, --table-settings: pdfplumber table settings as
        comma-separated key=value pairs.
        Example: -s snap_x_tolerance=5,join_x_tolerance=3
    -o, --output: Output file path. If omitted, writes to stdout.
    -d, --debug: Print one line per raw extracted table to stderr
        at the end of the run, showing raw table number, page, and
        first row. Repeat as -dd to also print grouped first-row
        statistics.

Known limitations:
    Column boundary x-drift: pdfplumber infers column positions from
    vertical line segments whose x-coordinates can vary slightly between
    pages and even within a single table. No single snap_x_tolerance value
    corrects all tables; tuning it is a global trade-off between fixing some
    tables and breaking others.

    Text-based column detection (vertical_strategy=text) is not a reliable
    fix for x-drift: it fragments tables more aggressively due to character
    spacing jitter, and its tuning knobs (text_x_tolerance,
    min_words_vertical) have the same cross-page variance problem.  In
    particular, min_words_vertical must be no greater than the row count of
    the smallest table, which constrains how much spurious boundary
    filtering is possible.
"""

import argparse
import csv
import re
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


def normalize_row_text(values):
    """Return row text with internal whitespace collapsed.

    Args:
        values: Iterable of cell or word strings.

    Returns:
        str: Normalized row text.
    """
    text = " ".join(clean_cell(value) for value in values if clean_cell(value))
    return re.sub(r"\s+", " ", text).strip()


def get_text_tolerances(table_settings):
    """Return text extraction tolerances for word grouping.

    Args:
        table_settings: Optional dict of pdfplumber table settings.

    Returns:
        tuple: ``(x_tolerance, y_tolerance)`` for ``extract_words()``.
    """
    if not table_settings:
        return 3, 3

    text_tolerance = table_settings.get("text_tolerance", 3)
    x_tolerance = table_settings.get("text_x_tolerance", text_tolerance)
    y_tolerance = table_settings.get("text_y_tolerance", text_tolerance)
    return x_tolerance, y_tolerance


def extract_word_lines(page, bbox, table_settings):
    """Return words from bbox grouped into visual lines.

    Args:
        page: A pdfplumber page.
        bbox: Table bounding box as ``(x0, top, x1, bottom)``.
        table_settings: Optional dict of pdfplumber table settings.

    Returns:
        list: List of lines, each a list of word dicts ordered by x.
    """
    x_tolerance, y_tolerance = get_text_tolerances(table_settings)
    cropped_page = page.crop(bbox)
    words = cropped_page.extract_words(
        x_tolerance=x_tolerance,
        y_tolerance=y_tolerance,
        keep_blank_chars=False,
    )

    lines = []
    for word in sorted(words, key=lambda item: (item["top"], item["x0"])):
        if not lines:
            lines.append(
                {
                    "top": word["top"],
                    "words": [word],
                },
            )
            continue
        if abs(lines[-1]["top"] - word["top"]) <= y_tolerance:
            lines[-1]["words"].append(word)
        else:
            lines.append(
                {
                    "top": word["top"],
                    "words": [word],
                },
            )

    return [
        sorted(line["words"], key=lambda item: item["x0"])
        for line in lines
    ]


def get_col_starts(page, bbox, table_settings):
    """Return column start x-positions from the first text line in bbox.

    Args:
        page: A pdfplumber page.
        bbox: Table bounding box as ``(x0, top, x1, bottom)``.
        table_settings: Optional dict of pdfplumber table settings.

    Returns:
        list: Ordered x-positions of column starts derived from the
            first visible text line, or an empty list if no words are
            found in bbox.
    """
    word_lines = extract_word_lines(page, bbox, table_settings)
    if not word_lines:
        return []
    return [word["x0"] for word in word_lines[0]]


def build_row_from_words(words, col_starts):
    """Return one row by assigning words to column intervals.

    Args:
        words: List of word dicts sorted by x position.
        col_starts: Ordered list of column start x positions.

    Returns:
        list: Row cells as strings.
    """
    row = [""] * len(col_starts)
    for word in words:
        col_index = len(col_starts) - 1
        for i in range(len(col_starts) - 1):
            if word["x0"] < col_starts[i + 1]:
                col_index = i
                break
        text = clean_cell(word["text"])
        if not text:
            continue
        if row[col_index]:
            row[col_index] = f"{row[col_index]} {text}"
        else:
            row[col_index] = text
    return row


def merge_rebuilt_rows(rows):
    """Merge wrapped continuation lines into the previous rebuilt row.

    Args:
        rows: List of rebuilt rows.

    Returns:
        list: Rebuilt rows with continuation lines merged.
    """
    merged = []
    for row in rows:
        non_empty_indexes = [i for i, cell in enumerate(row) if cell]
        if merged and non_empty_indexes and non_empty_indexes[0] > 0:
            for i in non_empty_indexes:
                if merged[-1][i]:
                    merged[-1][i] = f"{merged[-1][i]}\n{row[i]}"
                else:
                    merged[-1][i] = row[i]
        else:
            merged.append(row)
    return [row for row in merged if any(cell for cell in row)]


def rebuild_rows_from_words(page, bbox, rows, col_starts, table_settings):
    """Return rows rebuilt by assigning words to column intervals.

    Rebuilds all rows unconditionally using the given column positions.
    Use when the correct column layout is already known, such as when
    rebuilding continuation pages using column positions carried forward
    from a previous header page.

    Args:
        page: A pdfplumber page.
        bbox: Table bounding box as ``(x0, top, x1, bottom)``.
        rows: Raw table rows, returned unchanged if no words are found.
        col_starts: Ordered list of column start x-positions.
        table_settings: Optional dict of pdfplumber table settings.

    Returns:
        list: Rebuilt rows, or the original rows if no words are found
            in bbox or the rebuild yields no rows.
    """
    word_lines = extract_word_lines(page, bbox, table_settings)
    if not word_lines:
        return rows
    candidate_rows = [
        build_row_from_words(words, col_starts)
        for words in word_lines
    ]
    candidate_rows = merge_rebuilt_rows(candidate_rows)
    return candidate_rows if candidate_rows else rows


def rebuild_table_rows_from_text(page, bbox, rows, col_starts,
                                 table_settings):
    """Return repaired rows when the raw cell grid disagrees with text.

    Compares the first raw row against the first visible text line in
    bbox. Rebuilds using col_starts only when they disagree, and keeps
    the result only if its first row matches the first text line.

    Args:
        page: A pdfplumber page.
        bbox: Table bounding box as ``(x0, top, x1, bottom)``.
        rows: Raw table rows returned by pdfplumber.
        col_starts: Ordered list of column start x-positions to use
            when rebuilding.
        table_settings: Optional dict of pdfplumber table settings.

    Returns:
        list: Rebuilt rows when the fallback improves the first row;
            otherwise the original rows.
    """
    rebuilt_rows = rows
    if not rows:
        return rebuilt_rows

    word_lines = extract_word_lines(page, bbox, table_settings)
    if not word_lines:
        return rebuilt_rows

    template_words = word_lines[0]
    template_texts = [clean_cell(word["text"]) for word in template_words]
    template_texts = [text for text in template_texts if text]
    if not template_texts:
        return rebuilt_rows

    first_row = [clean_cell(cell) for cell in rows[0]]
    row_matches_template = (
        len(first_row) == len(template_texts)
        and normalize_row_text(first_row) == normalize_row_text(
            template_texts,
        )
    )
    if not row_matches_template:
        if not col_starts:
            return rebuilt_rows
        candidate_rows = [
            build_row_from_words(words, col_starts)
            for words in word_lines
        ]
        candidate_rows = merge_rebuilt_rows(candidate_rows)
        if candidate_rows:
            rebuilt_first = [clean_cell(cell) for cell in candidate_rows[0]]
            rebuilt_matches_template = (
                len(rebuilt_first) == len(template_texts)
                and normalize_row_text(rebuilt_first)
                == normalize_row_text(template_texts)
            )
            if rebuilt_matches_template:
                rebuilt_rows = candidate_rows
    return rebuilt_rows


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
    """Extract tables using pdfplumber's ``find_tables()``.

    Args:
        pdf: An open pdfplumber.PDF object.
        start_page: First page to extract (1-based, inclusive).
        end_page: Last page to extract (1-based, inclusive).
        table_settings: Optional dict of pdfplumber table
            settings passed to ``find_tables()``.

    Returns:
        list: List of dicts, each with keys 'page' (1-based page
            number), 'bbox' (table bounding box), and 'rows' (list of
            lists of cell values).
    """
    tables = []
    last_col_starts = []
    last_header_text = None
    for page_num in range(start_page, end_page + 1):
        page = pdf.pages[page_num - 1]
        page_tables = page.find_tables(table_settings)
        for table in page_tables:
            rows = table.extract()
            # Filter out empty rows
            rows = [
                row
                for row in rows
                if any(cell and cell.strip() for cell in row)
            ]
            if rows:
                first_row_text = normalize_row_text(rows[0])
                is_continuation = (
                    last_header_text is not None
                    and first_row_text != last_header_text
                )
                if is_continuation and last_col_starts:
                    rows = rebuild_rows_from_words(
                        page,
                        table.bbox,
                        rows,
                        last_col_starts,
                        table_settings,
                    )
                else:
                    col_starts = get_col_starts(
                        page, table.bbox, table_settings,
                    )
                    if col_starts:
                        last_col_starts = col_starts
                        last_header_text = first_row_text
                    rows = rebuild_table_rows_from_text(
                        page,
                        table.bbox,
                        rows,
                        col_starts,
                        table_settings,
                    )
                tables.append(
                    {
                        "page": page_num,
                        "bbox": table.bbox,
                        "rows": rows,
                    },
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
            f"{len(first_row)} cols [{cells}]\n",
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
        help="Merge tables that are split across page boundaries",
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
