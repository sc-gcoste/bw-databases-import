import re

import brightway2 as bw
from tqdm import tqdm

from custom_import_migrations import (
    wfldb_technosphere_migration_data,
    agb_technosphere_migration_data,
    afp_technosphere_migration_data,
    auslci_technosphere_migration_data,
)


def add_simapro_categories(importer):
    for process in importer:
        if "exchanges" in process:
            products = [x for x in process["exchanges"] if x["type"] == "production"]
            assert len(products) == 1
            product = products[0]
            process["simapro_categories"] = product["categories"]


def fix_locations(importer):
    for process in tqdm(importer):
        if not process.get("name"):
            continue

        if not process.get("location"):
            # Looking for stuff like xxxxxxxxxx{GLO}xxxxxxxxxxx

            pattern = r"{.+}"
            locations = [x for x in re.findall(pattern, process["name"])]
            if len(locations) == 1:
                process["location"] = locations[0][1:-1]  # Stripping the brackets
                continue

            pattern = r"; french production"
            if re.search(pattern, process["name"], re.IGNORECASE):
                process["location"] = "FR"
                continue

            pattern = r"/FR( \[Ciqual|$)"
            if re.search(pattern, process["name"]):
                process["location"] = "FR"
                continue

            pattern = r"/(?P<location>.+) U"
            locations = [x for x in re.findall(pattern, process["name"])]
            if len(locations) == 1:
                process["location"] = locations[0]
                continue

            pattern = r"(?P<location>\w+) ?/ ?U$"
            locations = [x for x in re.findall(pattern, process["name"])]
            if len(locations) == 1:
                process["location"] = locations[0]
                continue

            pattern = r"\W(?P<location>(([A-Z]{2,3})|(Europe without Switzerland)|(RoW)))(\W|$)"
            locations = [x[0] for x in re.findall(pattern, process["name"])]
            if len(locations) == 1:
                process["location"] = locations[0]
                continue

            pattern = r"/(?P<location>[A-Z]{2,3})$"
            locations = [x for x in re.findall(pattern, process["name"])]
            if len(locations) == 1:
                process["location"] = locations[0]
                continue

            print(
                f"No location found for process, used GLO instead.\n{process['name']}\n"
            )

            process["location"] = "GLO"


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

        fix_locations(agb_importer)

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

        for process in wfldb_importer:
            if not process.get("name"):
                if "Process name" in process["simapro metadata"]:
                    process["name"] = process["simapro metadata"]["Process name"]
                else:
                    prod_exchanges = [
                        x for x in process["exchanges"] if x.get("type") == "production"
                    ]
                    if len(prod_exchanges) != 1:
                        raise Exception("Unable to define process name")
                    process["name"] = prod_exchanges[0]["name"]

        wfldb_importer.apply_strategies()
        wfldb_importer.statistics()

        # The only unlinked process is Wastewater, average {CH}| treatment of, capacity 5E9l/year | Cut-off, S - Copied from ecoinvent
        # But it is only used by an orphan process, so it is considered ok to ignore it.

        wfldb_importer.add_unlinked_flows_to_biosphere_database()
        wfldb_importer.add_unlinked_activities()
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

        fix_locations(wfldb_importer)

        wfldb_importer.write_database()


def import_agrifootprint(delete_if_exist: bool = False):
    if delete_if_exist:
        if "agrifootprint" in bw.databases:
            del bw.databases["agrifootprint"]

    importer = bw.SimaProCSVImporter("databases/agrifootprint.CSV", "agrifootprint")

    technosphere_migration = bw.Migration("afp-technosphere")
    technosphere_migration.write(
        afp_technosphere_migration_data,
        description="Specific technosphere fixes for Agrifootprint",
    )

    importer.apply_strategies()
    importer.migrate("afp-technosphere")
    importer.apply_strategies()
    importer.statistics()
    importer.add_unlinked_activities()
    importer.add_unlinked_flows_to_biosphere_database()
    importer.statistics()

    fix_locations(importer)

    add_simapro_categories(importer)

    importer.write_database()


def import_auslci(delete_if_exist: bool = False):
    if delete_if_exist:
        if "auslci" in bw.databases:
            del bw.databases["auslci"]

    importer = bw.SimaProCSVImporter("databases/auslci.CSV", "auslci")

    technosphere_migration = bw.Migration("auslci-technosphere")
    technosphere_migration.write(
        auslci_technosphere_migration_data,
        description="Specific technosphere fixes for ausLCI",
    )

    importer.apply_strategies()
    importer.migrate("auslci-technosphere")
    importer.statistics()
    importer.add_unlinked_activities()
    importer.add_unlinked_flows_to_biosphere_database()
    importer.statistics()

    fix_locations(importer)

    add_simapro_categories(importer)

    importer.write_database()


def main():
    bw.projects.set_current("EF calculation")
    bw.bw2setup()

    import_ecoinvent("3.8")
    import_agribalyse()
    import_wfldb()
    import_agrifootprint()
    import_auslci()


if __name__ == "__main__":
    main()
