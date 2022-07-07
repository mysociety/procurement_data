import pandas as pd
import requests
from pathlib import Path

from data_common.local_authority import add_local_authority_code, add_region_and_county, add_gss_codes, add_extra_authority_info
from data_common.csv import replace_csv_headers

CONTRACT_XLS_URL = "https://fra1.digitaloceanspaces.com/ocdsdata/united_kingdom_contracts_finder_records/ocdsdata_united_kingdom_contracts_finder_records.xlsx"

top_level = Path(__file__).parent.parent.parent
raw_xls_path = top_level / "data" / "raw" / "contracts.xlsx"
interim_root = top_level / "data" / "interim"
package_root = top_level / "data" / "packages" / "procurement_data"

processed_csv_path = top_level / "data" / "packages" / "contracts" / "contracts.csv"

def get_excel_file():
    r = requests.get(CONTRACT_XLS_URL)
    with open(raw_xls_path, "wb") as out:
        out.write(r.content)

def split_sheets_to_csv():
    xls = pd.ExcelFile(raw_xls_path)
    for sheet_name in xls.sheet_names:
        if sheet_name != "Field Information":
            sheet = pd.read_excel(raw_xls_path, sheet_name)
            outfile = interim_root / "{}.csv".format(sheet_name)
            sheet.to_csv(outfile, index=False, header=True)

def tidy_buyer_csv():
    buyer_path = interim_root / "buyer.csv"
    df = pd.read_csv(buyer_path)
    cols = df.columns
    cols_to_keep = [ "_link", "_link_release", "name" ]
    cols_to_drop = [ col for col in cols if col not in cols_to_keep ]
    df = df.drop(columns=cols_to_drop)
    df.to_csv(buyer_path, index=False, header=True)

    replace_csv_headers(
        csv_file=buyer_path,
        new_headers=[
            "_link",
            "_link_release",
            "council",
        ],
        outfile=buyer_path,
    )

def add_council_codes():
    buyer_path = interim_root / "buyer.csv"
    df = pd.read_csv(buyer_path)
    df = add_local_authority_code(df=df)
    df.to_csv(buyer_path, index=False, header=True)

def remove_non_council_rows():
    merged_path = interim_root / "merged.csv"
    df = pd.read_csv(merged_path)
    df = df.loc[~df["local-authority-code"].isna()]
    df.to_csv(merged_path, index=False, header=True)

def merge_contract_data():
    buyer_path = interim_root / "buyer.csv"
    tender_items_path = interim_root / "tender_items.csv"
    tender_path = interim_root / "tender.csv"
    award_path = interim_root / "awards.csv"
    out_path = package_root / "merged.csv"

    items_df = pd.read_csv(tender_items_path)
    tender_df = pd.read_csv(tender_path)
    buyer_df = pd.read_csv(buyer_path)
    award_df = pd.read_csv(award_path)

    items_df = items_df.merge(buyer_df, on="_link_release", how="left", suffixes=("_item", "_buyer"))
    items_df = items_df.merge(tender_df, on="_link_release", how="left", suffixes=("_item", "_tender"))
    items_df = items_df.merge(award_df, on="_link_release", how="left", suffixes=("_item", "_award"))

    items_df.to_csv(out_path, index=False, header=True)

def build():
    get_excel_file()
    split_sheets_to_csv()
    tidy_buyer_csv()
    add_council_codes()
    merge_contract_data()
    remove_non_council_rows()


if __name__ == "__main__":
    build()
