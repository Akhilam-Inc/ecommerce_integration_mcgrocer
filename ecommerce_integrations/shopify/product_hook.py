import frappe

def set_shopify_update_flags(doc, method=None):
    """
    Set update flags in custom field before saving to determine what Shopify calls to make.
    This runs before save/insert to compare changes and set appropriate flags.
    """
    from ecommerce_integrations.shopify.constants import SETTING_DOCTYPE, MODULE_NAME
    import json
    
    setting = frappe.get_doc(SETTING_DOCTYPE)

    if not setting.is_enabled() or not setting.upload_erpnext_items:
        return
    
    # Default flags - sync everything for new items
    default_flags = {
        'update_product': True,
        'update_default_variant': True,
        'update_variant': True,
        'update_collections': True,
        'update_metafields': True,
        'update_images': True,
    }
    
    # For new items, set all flags to True
    if doc.is_new():
        doc.update_flags = json.dumps(default_flags)
        return

    ecommerce_item_exists = frappe.db.exists(
        "Ecommerce Item",
        {"erpnext_item_code": doc.name,}
    )

    if not ecommerce_item_exists:
        doc.update_flags = json.dumps(default_flags)
        return

    # For existing items, check what changed
    flags = {
        'update_product': False,
        'update_default_variant': False,
        'update_variant': False,
        'update_collections': False,
        'update_metafields': False,
        'update_images': False,
    }
    
    # Get old document to compare changes
    old_doc = doc.get_doc_before_save()
    if not old_doc:
        doc.update_flags = json.dumps(default_flags)
        return
    
    # Product-level fields
    product_fields = ['item_name', 'raw_html_description', 'item_group', 'disabled']
    for field in product_fields:
        if old_doc.get(field) != doc.get(field):
            flags['update_product'] = True
            break
    
    # Variant-level fields
    variant_fields = ['item_code', 'weight_per_unit', 'volumetric_weight', 'weight_uom', 'is_stock_item']
    from ecommerce_integrations.shopify.constants import ITEM_SELLING_RATE_FIELD
    variant_fields.append(ITEM_SELLING_RATE_FIELD)
    
    for field in variant_fields:
        if old_doc.get(field) != doc.get(field):
            if doc.variant_of:
                flags['update_variant'] = True
            else:
                flags['update_default_variant'] = True
            break
    
    # Check supplier changes (affects vendor)
    old_main_vendor = None
    new_main_vendor = None
    
    for supplier_data in old_doc.get('supplier_items', []):
        if supplier_data.get('main_vendor'):
            old_main_vendor = supplier_data.supplier
            break
    
    for supplier_data in doc.get('supplier_items', []):
        if supplier_data.get('main_vendor'):
            new_main_vendor = supplier_data.supplier
            break
    
    if old_main_vendor != new_main_vendor:
        flags['update_product'] = True
    
    # Check image changes
    if old_doc.get('image') != doc.get('image'):
        flags['update_images'] = True
    
    # Check collections (item_group changes)
    if old_doc.get('custom_website_breadcrumb') != doc.get('custom_website_breadcrumb'):
        flags['update_collections'] = True
    
    # Check metafields
    metafield_fields = ['original_description', 'original_name']
    for field in metafield_fields:
        if old_doc.get(field) != doc.get(field):
            flags['update_metafields'] = True
            break
    
    doc.update_flags = json.dumps(flags)