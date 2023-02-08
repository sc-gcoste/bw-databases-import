unharmonized_activities = {
    ('testing', 'Process with non standard unit'): {
        'name': 'Process with non standard unit',
        'unit': 'kilogram',
        'exchanges': [
            {
                'input': ('testing', 'Process with non standard unit'),
                'amount': 1,
                'unit': 'kilogram',
                'type': 'production'
            },
            {
                'input': ('biosphere3', 'c1b91234-6f24-417b-8309-46111d09c457'),  # Nitrogen oxides in air
                'amount': 5,
                'unit': 'lb',
                'type': 'biosphere'
            },
            {
                'input': ('testing', 'Uncategorized input process'),
                'amount': 2,
                'unit': 'tons',
                'type': 'technosphere'
            }
        ]
    }
}
