ENERGY_TEMPLATE_CSV = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
electricity,2,,
natural_gas,1,,
diesel,1,,
"""

PRODUCTION_TEMPLATE_CSV = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered
SKU-1,,,,1
SKU-2,,,,1
SKU-3,,,,0
"""

ENERGY_DEMO_CSV = """energy_carrier,scope,activity_amount,emission_factor_kgco2_per_unit
natural_gas,1,1000,2.00
diesel,1,200,2.68
electricity,2,5000,0.40
electricity,2,2000,0.35
"""

PRODUCTION_DEMO_CSV = """sku,quantity,export_to_eu_quantity,input_emission_factor_kg_per_unit,cbam_covered
SKU-A,1000,200,1.20,1
SKU-B,500,50,0.80,1
SKU-C,200,0,2.00,0
SKU-D,300,100,1.50,1
"""
