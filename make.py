import logging
from calendar import month_name
from pathlib import Path

import numpy as np
import pandas as pd
from etoolbox.datazip import DataZip
from etoolbox.utils.pudl import PretendPudlTablCore, make_pudl_tabl, read_pudl_table, TABLE_NAME_MAP
from etoolbox.utils.pudl_helpers import (
    fix_eia_na,
    remove_leading_zeros_from_numeric_strings,
    simplify_columns,
)
from pudl.extract import excel
from pudl.helpers import label_map, organize_cols
from pudl.metadata.classes import Package
from pudl.metadata.codes import CODE_METADATA
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

path = Path(__file__).parent
logger = logging.getLogger(__name__)


def main():
    tables = (
        "bf_eia923",
        "bga_eia860",
        "boil_eia860",
        "epacamd_eia",
        "epacamd_eia_subplant_ids",
        "frc_eia923",
        "gen_fuel_by_generator_eia923",
        "gen_fuel_by_generator_energy_source_eia923",
        # big and unnecessary
        # "gen_fuel_by_generator_energy_source_owner_eia923",
        "gen_original_eia923",
        "gens_eia860",
        "gf_eia923",
        # "gf_nonuclear_eia923",
        # "gf_nuclear_eia923",
        "own_eia860",
        "plants_eia860",
        "utils_eia860",
        "fuel_cost",
        "plant_parts_eia",
    )
    out = make_pudl_tabl(
        path / "temp",
        tables=tables,
        freq="MS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )
    for table in tables:
        if table in ("fuel_cost", "plant_parts_eia", "gf_nonuclear_eia923", "gf_nuclear_eia923"):
            continue
        if TABLE_NAME_MAP.get(table, table) not in out._dfs:
            out._dfs[table] = read_pudl_table(table)
    out._dfs["gens_eia860m"] = eia860m(
        (2022, 3), (2022, 5), (2023, 3), (2023, 5)
        # *[(a, b) for b in range(2023, 2015, -1) for a in (12, 9, 6, 3)]
    )

    DataZip.dump(out, path / "pdltbl2.zip")
    (path / "temp.zip").unlink()


def add(tables=("epacamd_eia_subplant_ids",)):
    obj = DataZip.load(path / "pdltbl.zip", klass=PretendPudlTablCore)
    for table in tables:
        obj._dfs[table] = read_pudl_table(table)
    DataZip.dump(obj, path / "pdltbl2.zip")


def add_860m():
    obj = DataZip.load(path / "pdltbl.zip", klass=PretendPudlTablCore)
    obj._dfs["gens_eia860m"] = eia860m(
        (2023, 3)
        # *[(a, b) for b in range(2023, 2015, -1) for a in (12, 9, 6, 3)]
    )
    DataZip.dump(obj, path / "pdltbl2.zip")


def eia860m(*args):
    def url(yr_, mo_, arc):
        return (
            f"https://www.eia.gov/electricity/data/eia860m{arc}/xls/"
            f"{month_name[mo_].casefold()}_generator{yr_}.xlsx"
        )

    def dl(yr_, mo_):
        try:
            return pd.ExcelFile(url(yr_, mo_, "/archive"))
        except ValueError:
            return pd.ExcelFile(url(yr_, mo_, ""))

    meta = excel.Metadata("eia860m")
    valid_parts = meta._file_name.columns
    dfs = []
    with logging_redirect_tqdm():
        for yr, mo in tqdm(args):
            if yr == 2015 and mo < 7:
                continue
            try:
                xl = dl(yr, mo)
            except ValueError:
                logger.warning("Cannot download EIA 860m for %s-%s", yr, mo)
                continue
            part = (
                f"{yr}-{mo:02}" if f"{yr}-{mo:02}" in valid_parts else max(valid_parts)
            )

            for page in meta.get_all_pages():
                df = (
                    xl.parse(
                        sheet_name=meta.get_sheet_name(page, year_month=part),
                        skiprows=meta.get_skiprows(page, year_month=part),
                        skipfooter=meta.get_skipfooter(page, year_month=part),
                        dtype={"Plant ID": pd.Int64Dtype()},
                    )
                    .pipe(simplify_columns)
                    .rename(columns=meta.get_column_map(page, year_month=part))
                    .assign(
                        report_year=yr,
                        report_month=mo,
                        report_date=pd.to_datetime(
                            f"{yr}-{mo:02}-01", format="%Y/%m/%d"
                        ),
                    )
                )
                for col in ["generator_id", "boiler_id"]:
                    if col in df.columns:
                        df = remove_leading_zeros_from_numeric_strings(
                            df=df, col_name=col
                        )
                dfs.append(df)

    gens_df = (
        pd.concat(dfs, axis=0, ignore_index=True)
        .assign(
            operational_status_code=lambda x: x.operational_status_code.fillna("RE")
        )
        .dropna(subset=["generator_id", "plant_id_eia"])
        .pipe(fix_eia_na)
    )

    columns_to_fix = (
        "planned_uprate_month",
        "planned_net_summer_capacity_uprate_mw",
        "planned_derate_year",
        "summer_capacity_mw",
        "planned_derate_month",
        "winter_capacity_mw",
        "planned_net_summer_capacity_derate_mw",
        "planned_uprate_year",
    )
    for column in columns_to_fix:
        gens_df[column] = gens_df[column].replace(to_replace=[" ", 0], value=np.nan)
    gens_df = (
        Package.from_resource_ids().get_resource("generators_eia860").encode(gens_df)
    )
    gens_df["operational_status"] = gens_df.operational_status_code.str.upper().map(
        label_map(
            CODE_METADATA["operational_status_eia"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )
    int_like = (
        lambda l: gens_df.filter(like=l)
        .dtypes.astype(str)
        .replace({"float64": "Int64"})
        .to_dict()
    )
    as_dt = lambda df, prefix: pd.to_datetime(
        df.rename(columns={f"{prefix}_year": "year", f"{prefix}_month": "month"})[
            ["year", "month", "day"]
        ],
        errors="coerce",
    )
    gens_df = gens_df.assign(
        day=1,
        generator_operating_date=lambda x: as_dt(x, "operating"),
        generator_retirement_date=lambda x: as_dt(x, "retirement"),
        current_planned_generator_operating_date=lambda x: as_dt(
            x, "current_planned_operating"
        ),
        planned_retirement_date=lambda x: as_dt(x, "planned_retirement"),
    )
    gens_df = gens_df.astype(int_like("_year") | int_like("_month"))
    gens_df = gens_df.drop(columns=["map_bing", "map_google", "day"])
    first_cols = [
        "report_date",
        "plant_id_eia",
        "plant_name_eia",
        "utility_id_eia",
        "utility_name_eia",
        "generator_id",
        "technology_description",
    ]
    return organize_cols(gens_df, first_cols)


if __name__ == "__main__":
    # add_860m()
    main()
