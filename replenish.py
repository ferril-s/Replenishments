"""FBA Replenishment Automation Script.

Reads BTS Calcs and Item List workbooks, computes case-vs-box pack-unit
quantities, and produces two output files:

  1. Amazon FBA Manifest  (Send-to-Amazon workflow upload)
  2. Warehouse Replenishment Sheet  (packing sheet for the warehouse)

Dependencies: pandas, openpyxl
Usage:        Place source files in the working directory and run:
              python replenish.py
"""

import math
import os
import re
from datetime import date

import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font

# ---------------------------------------------------------------------------
# Configuration — edit these if your folder layout differs
# ---------------------------------------------------------------------------

CONFIG = {
    "input_dir": ".",
    "output_dir": ".",
    "manifest_output": "FBA_Manifest_{date}.xlsx",
    "replenish_output": "WH_Replenishment_{date}.xlsx",
    "sample_size": None,  # None = process all rows where inv > 0
}

# Items whose Case Qty must be forced to a specific value,
# regardless of what the Item List spreadsheet contains.
CASE_QTY_OVERRIDES = {
    8004: 5,
    8005: 5,
    8006: 5,
}

# Alternating row colours for readability
FILL_LIGHT = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
FILL_WHITE = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
BOLD = Font(bold=True)


def _num(value, default=0.0):
    """Safely coerce *value* to float, returning *default* for NaN / None / non-numeric."""
    if value is None:
        return default
    try:
        f = float(value)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default

# ---------------------------------------------------------------------------
# Output path helper
# ---------------------------------------------------------------------------


def unique_path(path):
    """Return *path* if it doesn't exist; otherwise append _v2, _v3, … until free."""
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    version = 2
    while True:
        candidate = f"{base}_v{version}{ext}"
        if not os.path.exists(candidate):
            return candidate
        version += 1


# ---------------------------------------------------------------------------
# File scanning
# ---------------------------------------------------------------------------


def find_file(keywords, directory=None):
    """Locate a single .xlsx/.xls file whose name contains *all* given keywords.

    Parameters
    ----------
    keywords : str or list[str]
        Substrings that must ALL appear in the filename (case-insensitive).
        Underscores in a keyword also match the equivalent space.
    directory : str, optional
        Folder to scan.  Defaults to ``CONFIG["input_dir"]``.

    Returns
    -------
    str
        Full path to the matched file.

    Raises
    ------
    FileNotFoundError
        When no file matches the keywords.
    """
    if isinstance(keywords, str):
        keywords = [keywords]
    directory = directory or CONFIG["input_dir"]

    def _matches(filename):
        fn = filename.lower()
        for kw in keywords:
            lo = kw.lower()
            alt = kw.replace("_", " ").lower()
            if lo not in fn and alt not in fn:
                return False
        return True

    candidates = [
        f
        for f in os.listdir(directory)
        if f.lower().endswith((".xlsx", ".xls"))
        and not f.startswith("~$")
        and _matches(f)
    ]

    if not candidates:
        raise FileNotFoundError(
            f"No Excel file matching {keywords} found in "
            f"{os.path.abspath(directory)}"
        )

    if len(candidates) == 1:
        return os.path.join(directory, candidates[0])

    print(f"\nMultiple files match {keywords}:")
    for i, name in enumerate(candidates, 1):
        print(f"  {i}. {name}")
    while True:
        try:
            choice = int(input("Enter number: ")) - 1
            if 0 <= choice < len(candidates):
                return os.path.join(directory, candidates[choice])
        except ValueError:
            pass
        print("Invalid choice — try again.")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_sources():
    """Read every source workbook and return DataFrames + openpyxl workbooks.

    Returns a dict with keys:
        bts_df, item_df, data1_df, data2_df, instruction_df,
        replenish_wb, manifest_wb
    """
    bts_path = find_file("BTS_Calcs")
    item_path = find_file("Item_List")
    replenish_tpl_path = find_file(["Replenishment", "FBA"])
    manifest_tpl_path = find_file("ManifestFileUpload")

    print(f"  BTS Calcs     : {os.path.basename(bts_path)}")
    print(f"  Item List     : {os.path.basename(item_path)}")
    print(f"  Replenish Tpl : {os.path.basename(replenish_tpl_path)}")
    print(f"  Manifest Tpl  : {os.path.basename(manifest_tpl_path)}")

    bts_df = pd.read_excel(bts_path, sheet_name="Working Sheet")
    item_df = pd.read_excel(item_path, sheet_name="Sheet1")

    replenish_wb = load_workbook(replenish_tpl_path)
    data1_df = pd.read_excel(replenish_tpl_path, sheet_name="Data1")
    data2_df = pd.read_excel(replenish_tpl_path, sheet_name="Data2")
    instruction_df = pd.read_excel(replenish_tpl_path, sheet_name="Instruction")

    manifest_wb = load_workbook(manifest_tpl_path)

    return {
        "bts_df": bts_df,
        "item_df": item_df,
        "data1_df": data1_df,
        "data2_df": data2_df,
        "instruction_df": instruction_df,
        "replenish_wb": replenish_wb,
        "manifest_wb": manifest_wb,
    }


# ---------------------------------------------------------------------------
# Item-number extraction
# ---------------------------------------------------------------------------


def extract_item_number(sku):
    """Return the leading integer from an SKU after stripping any ``FBA_`` prefix.

    Examples::

        FBA_8006-CASE      -> 8006
        FBA_2227-EACH-UPC  -> 2227
        510-EACH-B         -> 510
    """
    if pd.isna(sku) or sku is None:
        return None
    clean = str(sku).replace("FBA_", "")
    m = re.match(r"(\d+)", clean)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# UoM parsing
# ---------------------------------------------------------------------------


def parse_uom(uom_str):
    """Convert a UoM string to the number of pieces per sellable unit.

    ``SET6`` -> 6,  ``EACH`` -> 1,  ``PACK2`` -> 2.
    Returns 1 when the string contains no digits.
    """
    if not uom_str or pd.isna(uom_str):
        return 1
    digits = re.search(r"\d+", str(uom_str))
    return int(digits.group()) if digits else 1


# ---------------------------------------------------------------------------
# Pack-unit selection
# ---------------------------------------------------------------------------


def select_pack_unit(inv_to_send, case_qty, box_qty, uom, case_dims, box_dims):
    """Decide CASE vs BOX and compute adjusted quantities and dimensions.

    Parameters
    ----------
    inv_to_send : float
        Number of **sellable sets** to ship (from BTS Calcs).
        NOT individual EA — multiply by *uom* to get EA.
    case_qty : float
        Individual pieces (EA) per case, from Item List.
    box_qty : float or None
        Individual pieces (EA) per box, from Item List.
    uom : int
        Pieces per sellable set (e.g. SET24 → 24, EACH → 1).
    case_dims, box_dims : dict or None
        Length / width / height / weight for the pack unit.

    Decision rules (all comparisons in EA)
    ---------------------------------------
    total_ea_raw = inv_to_send * uom
    threshold    = 0.8 * case_qty

    * total_ea_raw >= threshold             -> CASE  (round sets up to case multiple)
    * total_ea_raw <  threshold AND box_qty -> BOX   (closest box count to total_ea_raw)
    * total_ea_raw <  threshold AND no box  -> CASE  (minimum 1 full case)
    """
    inv_to_send = _num(inv_to_send)          # sellable sets
    case_qty = _num(case_qty, default=1.0)   # EA per case
    if case_qty == 0:
        case_qty = 1.0
    uom = max(1, int(_num(uom, default=1)))  # EA per set
    has_box = _num(box_qty) > 0

    total_ea_raw = inv_to_send * uom         # convert sets → EA
    threshold = 0.8 * case_qty

    if total_ea_raw >= threshold or not has_box:
        # --- CASE path ---
        # total_ea must be a multiple of BOTH case_qty (full cases) and
        # uom (whole sets).  The smallest valid EA step is LCM(case_qty, uom).
        cq = int(case_qty)
        step_ea = (cq * uom) // math.gcd(cq, uom)   # LCM
        step_sets = step_ea // uom                    # always whole

        lo_sets = max(step_sets, (math.floor(inv_to_send / step_sets)) * step_sets)
        hi_sets = lo_sets + step_sets

        # Pick whichever is closest to inv_to_send; tiebreak rounds up
        if abs(hi_sets - inv_to_send) <= abs(inv_to_send - lo_sets):
            adj_sets = hi_sets
        else:
            adj_sets = lo_sets

        total_ea = int(adj_sets * uom)
        num_packs = total_ea // cq
        dims = case_dims
        pack_type = "CASE"
        pack_qty = case_qty
    else:
        # --- BOX path ---
        # Same LCM logic: total_ea must satisfy both box_qty and uom.
        bq = int(float(box_qty))
        step_ea = (bq * uom) // math.gcd(bq, uom)   # LCM
        step_sets = step_ea // uom

        lo_sets = max(step_sets, (math.floor(inv_to_send / step_sets)) * step_sets)
        hi_sets = lo_sets + step_sets

        if abs(hi_sets - inv_to_send) <= abs(inv_to_send - lo_sets):
            adj_sets = hi_sets
        else:
            adj_sets = lo_sets

        total_ea = int(adj_sets * uom)
        num_packs = total_ea // bq
        dims = box_dims if box_dims else case_dims
        pack_type = "BOX"
        pack_qty = float(box_qty)

    return {
        "pack_type": pack_type,
        "pack_qty": int(pack_qty),
        "adj_sets": int(adj_sets),
        "total_ea": total_ea,
        "num_packs": num_packs,
        "length": math.floor(_num(dims.get("length"))),
        "width": math.floor(_num(dims.get("width"))),
        "height": math.floor(_num(dims.get("height"))),
        "weight": round(_num(dims.get("weight")), 2),
    }


# ---------------------------------------------------------------------------
# Instruction fallback
# ---------------------------------------------------------------------------


def extract_suffix(sku):
    """Extract the UoM-like suffix from an FBA SKU.

    ``FBA_2227-EACH-UPC`` -> ``EACH``,  ``FBA_582-SET6`` -> ``SET6``
    """
    clean = str(sku).replace("FBA_", "")
    stripped = re.sub(r"^\d+", "", clean).lstrip("-")
    return stripped.split("-")[0] if stripped else ""


def get_instruction_fallback(sku, instruction_df):
    """Match a packing instruction by UoM suffix when the SKU is missing.

    Looks for rows in the Instruction sheet whose ``FBA SKU`` column ends
    with the same suffix as *sku*.

    Returns
    -------
    dict or None
        ``{instruction_fba, instruction_wfs, suffix_used}`` on match,
        ``None`` otherwise.
    """
    suffix = extract_suffix(sku)
    if not suffix:
        return None

    fba_col = instruction_df["FBA SKU"].astype(str).str.upper()
    hits = instruction_df.loc[fba_col.str.endswith(suffix.upper())]
    if hits.empty:
        return None

    row = hits.iloc[0]
    return {
        "instruction_fba": row.get("Instruction FBA", ""),
        "instruction_wfs": row.get("Instruction WFS", ""),
        "suffix_used": suffix,
    }


# ---------------------------------------------------------------------------
# Worksheet-name helper
# ---------------------------------------------------------------------------


def find_sheet(wb, search_term):
    """Return the first worksheet whose name contains *search_term* (case-insensitive)."""
    for name in wb.sheetnames:
        if search_term.lower() in name.lower():
            return wb[name]
    raise KeyError(
        f"No sheet matching '{search_term}' in workbook.  "
        f"Available sheets: {wb.sheetnames}"
    )


# ---------------------------------------------------------------------------
# Output 1 — Amazon FBA Manifest
# ---------------------------------------------------------------------------


MANIFEST_HEADERS = [
    "Merchant SKU",       # A
    "Quantity",           # B
    "Prep owner",         # C  (values left blank)
    "Labeling owner",     # D  (values left blank)
    "Units per box",      # E
    "Number of boxes",    # F
    "Box length (in)",    # G
    "Box width (in)",     # H
    "Box height (in)",    # I
    "Box weight (lb)",    # J
    "Pack Unit",          # K
]


def write_manifest(manifest_wb, rows, output_path):
    """Create a clean manifest workbook using the Amazon Send-to-Amazon headers.

    Column headers match the official ManifestFileUpload template exactly.
    Prep owner (C) and Labeling owner (D) are left blank per Amazon defaults.
    Pack Unit (K) is appended so the warehouse knows CASE vs BOX.
    """
    out_wb = Workbook()
    ws = out_wb.active
    ws.title = "FBA Manifest"

    # Row 1: headers
    for col, header in enumerate(MANIFEST_HEADERS, start=1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = BOLD

    # Row 2+: data
    for idx, r in enumerate(rows):
        rn = 2 + idx
        ws.cell(row=rn, column=1, value=r["sku"])          # A  Merchant SKU
        ws.cell(row=rn, column=2, value=r["adj_sets"])      # B  Quantity (sellable units)
        # C  Prep owner — blank
        # D  Labeling owner — blank
        ws.cell(row=rn, column=5, value=r["pack_qty"])      # E  Units per box
        ws.cell(row=rn, column=6, value=r["num_packs"])     # F  Number of boxes
        ws.cell(row=rn, column=7, value=r["length"])        # G  Box length (in)
        ws.cell(row=rn, column=8, value=r["width"])         # H  Box width (in)
        ws.cell(row=rn, column=9, value=r["height"])        # I  Box height (in)
        ws.cell(row=rn, column=10, value=r["weight"])       # J  Box weight (lb)
        ws.cell(row=rn, column=11, value=r["pack_type"])    # K  Pack Unit

        fill = FILL_LIGHT if idx % 2 == 0 else FILL_WHITE
        for col in range(1, 12):
            ws.cell(row=rn, column=col).fill = fill

    # Auto-fit column widths for readability
    from openpyxl.utils import get_column_letter
    for col_idx in range(1, 12):
        max_len = max(
            len(str(ws.cell(row=r, column=col_idx).value or ""))
            for r in range(1, len(rows) + 2)
        )
        ws.column_dimensions[get_column_letter(col_idx)].width = max(max_len + 2, 12)

    out_wb.save(output_path)
    print(f"  Manifest saved  -> {output_path}")


# ---------------------------------------------------------------------------
# Output 2 — Warehouse Replenishment Sheet
# ---------------------------------------------------------------------------


def write_replenishment(replenish_wb, rows, output_path):
    """Fill the *For WH* sheet with data + formulas and save as a new file.

    Hard-coded columns: A (SKU), B (ASIN), C (FNSKU), P (Rec Qty), R (Pack Unit).
    All other columns are live Excel formulas that calculate when the file is
    opened in Excel.
    """
    ws = replenish_wb["For WH"]

    # Extra column headers
    ws.cell(row=1, column=17, value="Actual Qty Replenished").font = BOLD  # Q
    ws.cell(row=1, column=18, value="Current WH Inventory").font = BOLD    # R
    ws.cell(row=1, column=19, value="Pack Unit").font = BOLD               # S

    last_data_row = 1 + len(rows)

    for idx, r in enumerate(rows):
        rn = 2 + idx          # Excel row number
        rn_s = str(rn)        # stringified for formula interpolation

        # --- Hard-coded input columns ---
        ws.cell(row=rn, column=1, value=r["sku_no_prefix"])        # A  SKU
        ws.cell(row=rn, column=2, value=r["asin"])                 # B  ASIN
        ws.cell(row=rn, column=3, value=r["fnsku"])                # C  FNSKU
        ws.cell(row=rn, column=16, value=r["adj_sets"])            # P  Rec Replenishment Qty
        ws.cell(row=rn, column=17, value=r["inv_to_send"])        # Q  Actual Qty Replenished
        ws.cell(row=rn, column=19, value=r["pack_type"])           # S  Pack Unit

        # --- Formula columns ---
        ws.cell(row=rn, column=4,                                  # D  Item #
                value=f'=IFERROR(VLOOKUP(A{rn_s},Data1!A:B,2,0),"")')

        ws.cell(row=rn, column=5,                                  # E  Total EA
                value=f"=M{rn_s}*P{rn_s}")

        ws.cell(row=rn, column=6,                                  # F  Sellable Unit/Set
                value=f"=E{rn_s}/M{rn_s}")

        ws.cell(row=rn, column=7,                                  # G  Box to pull
                value=f'=IFERROR(E{rn_s}/N{rn_s},"")')

        ws.cell(row=rn, column=8,                                  # H  Case to pull
                value=f'=IFERROR(E{rn_s}/O{rn_s},"")')

        ws.cell(row=rn, column=9,                                  # I  Sets per Box/Case
                value=f'=IFERROR(F{rn_s}/H{rn_s},"")')

        ws.cell(row=rn, column=10,                                 # J  Label
                value=f'=IF(ISNUMBER(SEARCH("FNSKU",C{rn_s})),"Labeling","")')

        # K  Packing Instructions — direct value when fallback was used,
        #    otherwise a VLOOKUP formula that Excel will resolve.
        fb = r.get("instruction_fallback")
        if fb and fb.get("instruction_fba"):
            ws.cell(row=rn, column=11, value=fb["instruction_fba"])
        else:
            ws.cell(row=rn, column=11,
                    value=f'=IFERROR(VLOOKUP(A{rn_s},Instruction!A:F,6,0),"")')

        ws.cell(row=rn, column=12,                                 # L  Supplies
                value=f'=IFERROR(VLOOKUP(A{rn_s},Instruction!A:J,10,0),"No Supplies")')

        ws.cell(row=rn, column=13,                                 # M  UoM
                value=f'=IFERROR(VLOOKUP(A{rn_s},Data1!A:C,3,0),1)')

        ws.cell(row=rn, column=14,                                 # N  BOX
                value=f'=IFERROR(VLOOKUP(D{rn_s},Data2!A:D,4,0),"")')

        ws.cell(row=rn, column=15,                                 # O  CASE
                value=f'=IFERROR(VLOOKUP(D{rn_s},Data2!A:D,3,0),"")')

        ws.cell(row=rn, column=18, value="")                       # R  Current WH Inventory

        # Alternating row fill
        fill = FILL_LIGHT if idx % 2 == 0 else FILL_WHITE
        for col in range(1, 20):
            ws.cell(row=rn, column=col).fill = fill

    # --- Totals row ---
    tr = last_data_row + 1
    ws.cell(row=tr, column=1, value="TOTALS").font = BOLD
    for col_idx in (5, 6, 7, 8):  # E, F, G, H
        letter = chr(ord("A") + col_idx - 1)
        cell = ws.cell(row=tr, column=col_idx,
                       value=f"=SUM({letter}2:{letter}{last_data_row})")
        cell.font = BOLD

    # --- Update Table1 reference to cover header + data + totals ---
    try:
        table = ws.tables["Table1"]
        table.ref = f"A1:R{tr}"
    except (KeyError, AttributeError):
        for tname in list(ws.tables):
            ws.tables[tname].ref = f"A1:R{tr}"
            break

    replenish_wb.save(output_path)
    print(f"  Replenishment saved -> {output_path}")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(rows, missing_instructions, missing_pack_data):
    """Print a human-readable processing summary to stdout."""
    total_items = len(rows)
    total_ea = sum(r["total_ea"] for r in rows)
    cases = [r for r in rows if r["pack_type"] == "CASE"]
    boxes = [r for r in rows if r["pack_type"] == "BOX"]

    print(f"\n{'=' * 58}")
    print(f"  Total items processed  : {total_items}")
    print(f"  Total EA               : {total_ea:,}")
    print(f"  CASE decisions         : {len(cases)}")
    print(f"  BOX  decisions         : {len(boxes)}")

    if boxes:
        print("  Items shipped as BOX:")
        for r in boxes:
            print(f"    - {r['sku']}  ({r['total_ea']} EA, "
                  f"{r['num_packs']} box(es))")

    print(f"{'=' * 58}")

    if missing_instructions:
        print("\n  Instruction fallbacks used:")
        for m in missing_instructions:
            fba_val = m.get("instruction_fba", "N/A")
            print(f"    {m['sku']}  -> suffix '{m['suffix_used']}'  "
                  f"-> \"{fba_val}\"")

    if missing_pack_data:
        print("\n  WARNING — no pack-unit data found in Item List for:")
        for sku in missing_pack_data:
            print(f"    - {sku}")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """Entry point — orchestrates the full replenishment pipeline."""
    today = date.today().isoformat()

    print("=" * 58)
    print("  FBA Replenishment Automation")
    print(f"  Date : {today}")
    print(f"  Dir  : {os.path.abspath(CONFIG['input_dir'])}")
    print("=" * 58)

    # ---- 1. Load source files ----
    print("\nLocating source files ...")
    sources = load_sources()

    bts_df = sources["bts_df"]
    item_df = sources["item_df"]
    data1_df = sources["data1_df"]
    instruction_df = sources["instruction_df"]

    # ---- 2. Filter rows with inventory to send ----
    inv_col = "Inv to Send from Warehouse"
    bts_df = bts_df[bts_df[inv_col] > 0].copy()
    if CONFIG["sample_size"]:
        bts_df = bts_df.head(CONFIG["sample_size"])

    print(f"\n  Rows with inventory > 0 : {len(bts_df)}")

    # ---- 3. Extract item numbers & deduplicate ----
    bts_df["Item No."] = bts_df["Merchant SKU"].apply(extract_item_number)
    bts_df = bts_df.dropna(subset=["Item No."])
    bts_df["Item No."] = bts_df["Item No."].astype(int)

    bts_dupes = bts_df.duplicated(subset=["Merchant SKU"], keep="first")
    if bts_dupes.any():
        n = bts_dupes.sum()
        print(f"  NOTE: dropped {n} duplicate Merchant SKU rows from BTS Calcs")
    bts_df = bts_df.drop_duplicates(subset=["Merchant SKU"], keep="first")

    item_df["Item No."] = pd.to_numeric(item_df["Item No."], errors="coerce")
    item_df = item_df.dropna(subset=["Item No."])
    item_df["Item No."] = item_df["Item No."].astype(int)
    item_df = item_df.drop_duplicates(subset=["Item No."], keep="first")

    # ---- 4. Merge BTS with Item List on Item No. ----
    merged = bts_df.merge(item_df, on="Item No.", how="left")

    # ---- 5. Build UoM lookup from Data1 (col 0 = SKU, col 2 = UOM) ----
    sku_to_uom = dict(
        zip(
            data1_df.iloc[:, 0].astype(str),
            data1_df.iloc[:, 2].astype(str),
        )
    )

    # ---- 6. Set of SKUs present in the Instruction sheet ----
    instruction_skus = set(
        instruction_df.iloc[:, 0].astype(str).str.strip()
    )

    # ---- 7. Process every item ----
    rows: list[dict] = []
    missing_instructions: list[dict] = []
    missing_pack_data: list[str] = []

    for _, row in merged.iterrows():
        sku = str(row["Merchant SKU"])
        sku_no_prefix = sku.replace("FBA_", "")
        uom_str = sku_to_uom.get(sku_no_prefix, "EACH")
        uom = parse_uom(uom_str)

        has_case_qty = pd.notna(row.get("Case Qty"))
        has_dims = pd.notna(row.get("Case Length"))
        if not has_case_qty:
            label = "(has dimensions but no Case Qty)" if has_dims else "(not in Item List)"
            missing_pack_data.append(f"{sku}  {label}")

        case_dims = {
            "length": row.get("Case Length", 0),
            "width": row.get("Case Width", 0),
            "height": row.get("Case Height", 0),
            "weight": row.get("Case Weight", 0),
        }

        box_dims = (
            {
                "length": row.get("Box Length", 0),
                "width": row.get("Box Width", 0),
                "height": row.get("Box Height", 0),
                "weight": row.get("Box Weight", 0),
            }
            if pd.notna(row.get("Box Qty"))
            else None
        )

        item_no = int(row["Item No."])
        case_qty_val = CASE_QTY_OVERRIDES.get(item_no, row.get("Case Qty", 1))

        result = select_pack_unit(
            inv_to_send=row[inv_col],
            case_qty=case_qty_val,
            box_qty=row.get("Box Qty"),
            uom=uom,
            case_dims=case_dims,
            box_dims=box_dims,
        )

        result["sku"] = sku
        result["sku_no_prefix"] = sku_no_prefix
        result["inv_to_send"] = _num(row[inv_col])
        result["asin"] = (
            str(row["ASIN"]) if pd.notna(row.get("ASIN")) else ""
        )
        result["fnsku"] = (
            str(row["FNSKU"]) if pd.notna(row.get("FNSKU")) else ""
        )

        # Instruction fallback — only when SKU is absent from the sheet
        if sku_no_prefix not in instruction_skus:
            fallback = get_instruction_fallback(sku, instruction_df)
            if fallback:
                result["instruction_fallback"] = fallback
                missing_instructions.append({"sku": sku, **fallback})
            else:
                result["instruction_fallback"] = None
                print(f"  WARNING: no instruction match for {sku}")
        else:
            result["instruction_fallback"] = None

        rows.append(result)

    if not rows:
        print("\nNo items to process — exiting.")
        return

    # ---- 8. Write output files ----
    manifest_path = unique_path(os.path.join(
        CONFIG["output_dir"],
        CONFIG["manifest_output"].format(date=today),
    ))
    replenish_path = unique_path(os.path.join(
        CONFIG["output_dir"],
        CONFIG["replenish_output"].format(date=today),
    ))

    print("\nWriting output files ...")
    write_manifest(sources["manifest_wb"], rows, manifest_path)
    write_replenishment(sources["replenish_wb"], rows, replenish_path)

    # ---- 9. Summary ----
    print_summary(rows, missing_instructions, missing_pack_data)

    print(f"  Manifest  -> {os.path.abspath(manifest_path)}")
    print(f"  Replenish -> {os.path.abspath(replenish_path)}")
    print("\nDone.")


if __name__ == "__main__":
    main()
