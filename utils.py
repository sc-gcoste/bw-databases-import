import copy

import brightway2 as bw
from bw2io.units import UNITS_NORMALIZATION, DEFAULT_UNITS_CONVERSION

# Add here missing unit conversion in the form of:
# (from_unit, to_unit, conversion_factor)
ADDITIONAL_UNIT_CONVERSION = [
    ("galons(US liq)", "cubic meter", 0.003785412),
    (
        "litre",
        "cubic meter",
        1e-3,
    ),  # I don't know why but this one is commented in BW source code
]

unit_conversion = {
    (conversion[0], conversion[1]): conversion[2]
    for conversion in DEFAULT_UNITS_CONVERSION + ADDITIONAL_UNIT_CONVERSION
}

# Add here missing unit normalization in the form of:
# unit: reference_unit
ADDITIONAL_UNITS_NORMALIZATION = {
    "kilogramkilometer": "kilogram kilometer",
    "tonkilometer": "ton kilometer",
    "megawatthour": "megawatt hour",
    "kilowatthour": "kilowatt hour",
    "liter": "litre",
    "tons": "ton",
}
UNITS_NORMALIZATION.update(ADDITIONAL_UNITS_NORMALIZATION)

VALID_UNITS = (
    set([x for x in UNITS_NORMALIZATION.keys()])
    .union([x for x in UNITS_NORMALIZATION.values()])
    .union([x for x in ADDITIONAL_UNITS_NORMALIZATION.keys()])
    .union([x for x in ADDITIONAL_UNITS_NORMALIZATION.values()])
    .union([x[0] for x in DEFAULT_UNITS_CONVERSION])
    .union([x[1] for x in DEFAULT_UNITS_CONVERSION])
    .union([x[0] for x in ADDITIONAL_UNIT_CONVERSION])
    .union([x[1] for x in ADDITIONAL_UNIT_CONVERSION])
)


def normalize_units(unit: str) -> str:
    """
    Examples:
        >>> normalize_units('km')
        'kilometer'

    """
    return UNITS_NORMALIZATION.get(
        unit.lower(),
        unit.lower() if unit.lower() in UNITS_NORMALIZATION.values() else unit,
    )


def harmonize_exchange_units(activities: dict) -> dict:
    """
    Converts the exchanges units and amounts to fit to the default unit of the exchange.
    Returns a copy of the dataset with changed units and amounts.

    Args:
        activities (dict): Dictionary containing activities to import in a bw2 database.
    """

    activities = copy.deepcopy(activities)

    for activity in activities.values():
        for exchange in activity["exchanges"]:
            if exchange["type"] == "production":
                continue

            input_unit = exchange["unit"]

            # If the exchange is from the list of activities to import, check that the unit is valid
            if exchange["input"] in activities.keys():
                if normalize_units(exchange["unit"]) not in VALID_UNITS:
                    raise ValueError(
                        f"Invalid unit \"{exchange['unit']}\" for exchange \"{exchange['input']}\""
                    )
                reference_unit = normalize_units(exchange["unit"])
            # If the exchange is from a background process, get the reference from brightway
            else:
                reference_unit = bw.get_activity(exchange["input"])["unit"]

            if input_unit != reference_unit:
                if normalize_units(input_unit) != normalize_units(reference_unit):
                    try:
                        exchange["amount"] = (
                            exchange["amount"]
                            * unit_conversion[
                                normalize_units(input_unit),
                                normalize_units(reference_unit),
                            ]
                        )
                    except KeyError:
                        raise KeyError(
                            f"Unit conversion missing: "
                            f"{normalize_units(input_unit)} -> {normalize_units(reference_unit)}"
                        )

                exchange["unit"] = reference_unit

    return activities
