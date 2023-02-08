import brightway2 as bw

from utils import harmonize_exchange_units
from tests.test_data import unharmonized_activities


def test_harmonize_unit():
    bw.projects.set_current('Testing database')

    harmonized_activities = harmonize_exchange_units(unharmonized_activities)

    assert harmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][1]['unit'] == 'kilogram'
    assert harmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][1]['unit'] \
           != unharmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][1]['unit']
    assert harmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][2]['unit'] == 'kilogram'
    assert harmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][2]['unit'] \
           != unharmonized_activities[('testing', 'Process with non standard unit')]['exchanges'][2]['unit']

