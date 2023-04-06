import brightway2 as bw
from tqdm import tqdm

from custom_import_migrations import (
    wfldb_technosphere_migration_data,
    agb_technosphere_migration_data,
)


def add_simapro_categories(importer):
    for process in importer:
        if "exchanges" in process:
            products = [x for x in process["exchanges"] if x["type"] == "production"]
            assert len(products) == 1
            product = products[0]
            process["simapro_categories"] = product["categories"]


def _check_db_exists(db_name: str, delete_if_exist: bool):
    if db_name in bw.databases:
        if delete_if_exist:
            del bw.databases[db_name]
            return False
        else:
            print(f"{db_name} has already been imported.")
            return True


def import_ecoinvent(version: str = "3.8", delete_if_exist: bool = False):
    db_name = f"ecoinvent{version}_cutoff"

    if not _check_db_exists(db_name, delete_if_exist):
        fp_ei = f"databases/ecoinvent {version}_cutoff_ecoSpold02/datasets"
        ei = bw.SingleOutputEcospold2Importer(fp_ei, db_name, use_mp=False)
        ei.apply_strategies()
        ei.statistics()

        # Changing the process names to differentiate coproducts
        for activity in ei:
            if activity["name"] not in (
                activity["reference product"],
                "market for " + activity["reference product"],
            ):
                activity[
                    "name"
                ] = f"{activity['name']} - {activity['reference product']}"

        ei.write_database()


def import_agribalyse(delete_if_exist: bool = False):
    db_name = "agribalyse3"
    if not _check_db_exists(db_name, delete_if_exist):
        agb_csv_filepath = r"databases/agribalyse3_no_param.CSV"

        agb_importer = bw.SimaProCSVImporter(agb_csv_filepath, "agribalyse3")

        agb_technosphere_migration = bw.Migration("agb-technosphere")
        agb_technosphere_migration.write(
            agb_technosphere_migration_data,
            description="Specific technosphere fixes for Agribalyse 3",
        )

        agb_importer.apply_strategies()
        agb_importer.statistics()
        agb_importer.migrate("agb-technosphere")
        agb_importer.match_database(
            "ecoinvent3.8_cutoff",
            fields=("reference product", "location", "unit", "name"),
        )
        agb_importer.apply_strategies()
        agb_importer.statistics()

        # The only remaining unlinked exchanges are final waste flows and a land use change process. We will consider
        # that they can be ignored.

        agb_importer.add_unlinked_flows_to_biosphere_database()
        agb_importer.add_unlinked_activities()
        agb_importer.statistics()

        # Fixing uncertainty data
        for process in tqdm(agb_importer):
            for exchange in process.get("exchanges", []):
                if (
                    (exchange.get("uncertainty type") == 2)
                    and (exchange.get("scale", 0) <= 0)
                    or (exchange.get("uncertainty type") == 5)
                    and (exchange.get("minimum", 0) >= exchange.get("maximum", 0))
                ):
                    exchange["uncertainty type"] = 0

                    for to_delete in ["loc", "scale", "negative", "minimum", "maximum"]:
                        if to_delete in exchange:
                            del exchange[to_delete]

        add_simapro_categories(agb_importer)

        agb_importer.write_database()


def import_wfldb(delete_if_exist: bool = False):
    db_name = "WFLDB"
    if not _check_db_exists(db_name, delete_if_exist):
        wfldb_csv_filepath = r"databases/WFLDB_no_param.CSV"

        wfldb_importer = bw.SimaProCSVImporter(wfldb_csv_filepath, "WFLDB")

        wfldb_technosphere_migration = bw.Migration("wfldb-technosphere")
        wfldb_technosphere_migration.write(
            wfldb_technosphere_migration_data,
            description="Specific technosphere fixes for World Food DB",
        )

        wfldb_importer.apply_strategies()
        wfldb_importer.statistics()
        wfldb_importer.migrate("wfldb-technosphere")
        wfldb_importer.apply_strategies()
        wfldb_importer.statistics()

        # The only unlinked process is Wastewater, average {CH}| treatment of, capacity 5E9l/year | Cut-off, S - Copied from ecoinvent
        # But it is only used by an orphan process, so it is considered ok to ignore it.

        wfldb_importer.add_unlinked_activities()
        wfldb_importer.add_unlinked_flows_to_biosphere_database()
        wfldb_importer.statistics()

        # Fixing an invalid uncertainty value for 3 exchanges
        exchanges_to_fix = [
            (
                "Coffee, regular, freeze dried, at plant (WFLDB)",
                "Transformation into freeze-dried soluble coffee, green coffee, per kg product (WFLDB)",
            ),
            (
                "Coffee, regular, spray dried, at plant (WFLDB)",
                "Transformation into spray-dried soluble coffee, green coffee, per kg product (WFLDB)",
            ),
            (
                "Coffee, regular, roast and ground, at plant (WFLDB)",
                "Roasting and grinding, green coffee (WFLDB)",
            ),
        ]

        for activity_name, input_name in exchanges_to_fix:
            exchange = next(
                y
                for x in wfldb_importer
                for y in x["exchanges"]
                if (x["name"] == activity_name) and (y["name"] == input_name)
            )
            exchange["uncertainty type"] = 0
            del exchange["loc"]
            del exchange["scale"]
            del exchange["negative"]

        add_simapro_categories(wfldb_importer)

        wfldb_importer.write_database()


def main():
    bw.projects.set_current("EF calculation")
    bw.bw2setup()

    import_ecoinvent("3.8")
    import_agribalyse()
    import_wfldb()


if __name__ == "__main__":
    main()
