from pathlib import Path

from etoolbox.utils.pudl import (
    make_pudl_tabl,
    read_pudl_table,
    PretendPudlTabl,
    get_pudl_tables_as_dz,
)
from etoolbox.datazip import DataZip


path = Path(__file__).parent


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
        "gf_nonuclear_eia923",
        "gf_nuclear_eia923",
        "own_eia860",
        "plants_eia860",
        "utils_eia860",
    )
    out = make_pudl_tabl(
        path / "pdltbl_ms.zip",
        tables=tables,
        freq="MS",
        fill_fuel_cost=True,
        roll_fuel_cost=True,
        fill_net_gen=True,
    )


if __name__ == "__main__":
    main()
