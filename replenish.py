"""FBA Replenishment Automation Script.

Reads BTS Calcs and Item List workbooks, computes pack-unit quantities,
and produces an Amazon FBA Manifest and a Warehouse Replenishment sheet.
"""

import math
import os
import re
import sys
from datetime import date

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG = {
    "input_dir": ".",
    "output_dir": ".",
    "manifest_output": "FBA_Manifest_{date}.xlsx",
    "replenish_output": "WH_Replenishment_{date}.xlsx",
    "sample_size": None,  # None = all rows with inv > 0
}

# ---------------------------------------------------------------------------
# File scanning
# ---------------------------------------------------------------------------


def find_file(keyword: str, directory: str | None = None) -> str:
    """Locate a single .xlsx/.xls file whose name contains *keyword*.

    If multiple matches are found the user is prompted to choose.
    Raises FileNotFoundError when no match exists.
    """
    directory = directory or CONFIG["input_dir"]
    kw_lower = keyword.lower()
    kw_alt = keyword.replace("_", " ").lower()
    matches = [
        f
        for f in os.listdir(directory)
        if f.lower().endswith((".xlsx", ".xls"))
        and (kw_lower in f.lower() or kw_alt in f.lower())
    ]

    if not matches:
        raise FileNotFoundError(
            f"No Excel file matching '{keyword}' found in {os.path.abspath(directory)}"
        )

    if len(matches) == 1:
        return os.path.join(directory, matches[0])

    print(f"Multiple files match '{keyword}':")
    for i, name in enumerate(matches, 1):
        print(f"  {i}. {name}")
    choice = int(input("Enter number: ")) - 1
    return os.path.join(directory, matches[choice])


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_sources() -> dict:
    """Read all source workbooks and return raw DataFrames + template WBs."""
    bts_path = find_file("BTS_Calcs")
    item_path = find_file("Item_List")
    replenish_tpl_path = find_file("Replenishment")
    manifest_tpl_path = find_file("ManifestFileUpload")

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


def extract_item_number(sku: str) -> int | None:
    """Return the leading integer from an SKU after stripping the FBA_ prefix."""
    clean = sku.replace("FBA_", "")
    match = re.match(r"(\d+)", clean)
    return int(match.group(1)) if match else None


# ---------------------------------------------------------------------------
# UoM lookup
# ---------------------------------------------------------------------------


def parse_uom(uom_str: str) -> int:
    """Convert a UoM string like 'SET6', 'EACH', or 'PACK2' to an integer."""
    if not uom_str or pd.isna(uom_str):
        return 1
    digits = re.search(r"\d+", str(uom_str))
    return int(digits.group()) if digits else 1


# ---------------------------------------------------------------------------
# Pack-unit selection
# ---------------------------------------------------------------------------


def select_pack_unit(
    inv_to_send: float,
    case_qty: float,
    box_qty: float | None,
    uom: int,
    case_dims: dict,
    box_dims: dict | None,
) -> dict:
    """Decide CASE vs BOX and compute adjusted quantities + dimensions."""
    threshold = 0.8 * case_qty

    if inv_to_send >= threshold or box_qty is None or pd.isna(box_qty):
        sets_per_case = case_qty / uom
        raw_sets = math.ceil(inv_to_send / uom)
        adj_sets = math.ceil(raw_sets / sets_per_case) * sets_per_case
        total_ea = int(adj_sets * uom)
        num_packs = int(total_ea / case_qty) if case_qty else 1
        dims = case_dims
        pack_type = "CASE"
    else:
        num_packs = max(1, math.floor(inv_to_send / box_qty))
        total_ea = int(num_packs * box_qty)
        adj_sets = total_ea / uom
        dims = box_dims or case_dims
        pack_type = "BOX"

    return {
        "pack_type": pack_type,
        "pack_qty": case_qty if pack_type == "CASE" else box_qty,
        "adj_sets": adj_sets,
        "total_ea": total_ea,
        "num_packs": num_packs,
        "length": dims.get("length", 0),
        "width": dims.get("width", 0),
        "height": dims.get("height", 0),
        "weight": dims.get("weight", 0),
    }


# ---------------------------------------------------------------------------
# Instruction fallback
# ---------------------------------------------------------------------------


def get_instruction_fallback(sku: str, instruction_df: pd.DataFrame) -> dict | None:
    """Try to match a packing instruction by UoM suffix when VLOOKUP would fail."""
    clean = sku.replace("FBA_", "")
    stripped = re.sub(r"^\d+", "", clean).lstrip("-")
    suffix = stripped.split("-")[0] if stripped else ""
    if not suffix:
        return None

    mask = instruction_df["FBA SKU"].astype(str).str.upper().str.endswith(suffix.upper())
    hits = instruction_df.loc[mask]
    if hits.empty:
        return None

    row = hits.iloc[0]
    return {
        "instruction_fba": row.get("Instruction FBA", ""),
        "instruction_wfs": row.get("Instruction WFS", ""),
        "suffix_used": suffix,
    }


# ---------------------------------------------------------------------------
# Output writers (stubs – to be implemented in next phase)
# ---------------------------------------------------------------------------


def write_manifest(manifest_wb, rows: list[dict], output_path: str):
    """Write the Amazon FBA Manifest from template."""
    raise NotImplementedError("write_manifest not yet implemented")


def write_replenishment(replenish_wb, rows: list[dict], output_path: str):
    """Write the Warehouse Replenishment 'For WH' sheet from template."""
    raise NotImplementedError("write_replenishment not yet implemented")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def print_summary(rows: list[dict]):
    """Print a human-readable processing summary to the console."""
    total_items = len(rows)
    total_ea = sum(r["total_ea"] for r in rows)
    cases = [r for r in rows if r["pack_type"] == "CASE"]
    boxes = [r for r in rows if r["pack_type"] == "BOX"]

    print(f"\n{'=' * 50}")
    print(f"Total items processed : {total_items}")
    print(f"Total EA              : {total_ea}")
    print(f"CASE decisions        : {len(cases)}")
    print(f"BOX  decisions        : {len(boxes)}")
    if boxes:
        print("  BOX items:")
        for r in boxes:
            print(f"    - {r['sku']}")
    print(f"{'=' * 50}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    today = date.today().isoformat()

    print("FBA Replenishment Automation")
    print(f"Date: {today}\n")

    # 1. Load sources
    sources = load_sources()

    bts_df = sources["bts_df"]
    item_df = sources["item_df"]
    data1_df = sources["data1_df"]
    instruction_df = sources["instruction_df"]

    # 2. Filter to rows with inventory to send
    inv_col = "Inv to Send from Warehouse"
    bts_df = bts_df[bts_df[inv_col] > 0].copy()
    if CONFIG["sample_size"]:
        bts_df = bts_df.head(CONFIG["sample_size"])

    # 3. Extract item numbers
    bts_df["Item No."] = bts_df["Merchant SKU"].apply(extract_item_number)

    # 4. Merge with Item List
    merged = bts_df.merge(item_df, on="Item No.", how="left")

    # 5. UoM lookup via Data1
    sku_to_uom = dict(
        zip(data1_df.iloc[:, 0].astype(str), data1_df.iloc[:, 1].astype(str))
    )

    # 6. Process each item
    rows: list[dict] = []
    missing_instructions: list[dict] = []

    for _, row in merged.iterrows():
        sku = str(row["Merchant SKU"])
        sku_no_prefix = sku.replace("FBA_", "")
        uom_str = sku_to_uom.get(sku_no_prefix, "EACH")
        uom = parse_uom(uom_str)

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

        result = select_pack_unit(
            inv_to_send=row[inv_col],
            case_qty=row.get("Case Qty", 1),
            box_qty=row.get("Box Qty"),
            uom=uom,
            case_dims=case_dims,
            box_dims=box_dims,
        )
        result["sku"] = sku
        result["sku_no_prefix"] = sku_no_prefix
        result["asin"] = row.get("ASIN", "")
        result["fnsku"] = row.get("FNSKU", "")

        fallback = get_instruction_fallback(sku, instruction_df)
        if fallback:
            result["instruction_fallback"] = fallback
            missing_instructions.append({"sku": sku, **fallback})

        rows.append(result)

    # 7. Write outputs
    manifest_path = os.path.join(
        CONFIG["output_dir"], CONFIG["manifest_output"].format(date=today)
    )
    replenish_path = os.path.join(
        CONFIG["output_dir"], CONFIG["replenish_output"].format(date=today)
    )

    write_manifest(sources["manifest_wb"], rows, manifest_path)
    write_replenishment(sources["replenish_wb"], rows, replenish_path)

    # 8. Summary
    print_summary(rows)

    if missing_instructions:
        print("Instruction fallbacks used:")
        for m in missing_instructions:
            print(f"  {m['sku']} -> suffix '{m['suffix_used']}'")

    print(f"Manifest  -> {manifest_path}")
    print(f"Replenish -> {replenish_path}")


if __name__ == "__main__":
    main()
