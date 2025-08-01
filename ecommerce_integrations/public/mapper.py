# You can run this as a script to update payload.json in-place

import json

# Load products.json
with open('products.json', 'r') as f:
    products = json.load(f)

# Build a mapping from sku to title
sku_to_title = {}
for product in products:
    variants = product.get('variants', [])
    if not isinstance(variants, list):
        continue
    for variant in variants:
        sku = variant.get('sku')
        if sku:
            sku_to_title[sku] = product.get('title')

# Load payload.json
with open('payload.json', 'r') as f:
    payload = json.load(f)

# Update names in payload.json
for entry in payload:
    product_id = entry.get('product_id')
    if product_id and product_id in sku_to_title:
        # Replace non-breaking spaces with regular spaces
        name = sku_to_title[product_id]
        if name:
            entry['name'] = name.replace('\u00a0', ' ')

# Save the updated payload.json
with open('payload-new.json', 'w', encoding='utf-8') as f:
    json.dump(payload, f, indent=2, ensure_ascii=False)