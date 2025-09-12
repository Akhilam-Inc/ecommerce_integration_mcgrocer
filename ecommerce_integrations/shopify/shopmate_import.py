import frappe
import json
import os
import ast
import time
from ecommerce_integrations.shopify.utils import create_shopify_log
from erpnext.controllers.item_variant import create_multiple_variants

# Modified version of import_production_items and create_item_and_ecommerce_item
# Returns responses instead of printing, for use in Product Import Tool logging

def import_shopmate_items_from_json(data):
    """
    Import a list of Shopmate product dicts, creating them in Shopify and ERPNext.
    Returns a list of dicts: {item, created, updated, error}
    """
    results = []
    for product in data:
        try:
            res = create_item_and_ecommerce_item_return(product, integration="shopify")
            if res.get('error'):
                results.append({'item': product.get('title'), 'error': res['error']})
            elif res.get('created'):
                results.append({'item': product.get('title'), 'created': True})
            elif res.get('updated'):
                results.append({'item': product.get('title'), 'updated': True})
            else:
                results.append({'item': product.get('title'), 'info': 'Processed'})
        except Exception as e:
            results.append({'item': product.get('title'), 'error': str(e)})
    return results

def create_item_and_ecommerce_item_return(product, integration="shopify"):
    """
    Like create_item_and_ecommerce_item, but returns a dict with status and error info.
    """
    if product.get("multiple_variants"):
        return create_variant_product_return(product, integration=integration)
    else:
        return create_single_item_return(product, integration=integration)

def _create_or_get_item_attribute(attribute_name, attribute_values):
    """
    Ensures an Item Attribute and its values exist in ERPNext.
    Creates them if they don't.
    """
    import traceback
    try:
        # Filter out any None values from the incoming list to be safe.
        attribute_values = [v for v in attribute_values if v is not None]
        if not attribute_values:
            return

        if not frappe.db.exists("Item Attribute", attribute_name):
            attribute_doc = frappe.get_doc({ # pyright: ignore
                "doctype": "Item Attribute",
                "attribute_name": attribute_name,
            })
            for val in attribute_values:
                attribute_doc.append("item_attribute_values", {"attribute_value": val, "abbr": val})
            attribute_doc.insert(ignore_permissions=True)
        else:
            attribute_doc = frappe.get_doc("Item Attribute", attribute_name)
            existing_values = set()
            for d in attribute_doc.item_attribute_values:
                # Defensive check to ensure we only process valid strings
                if d and d.attribute_value and isinstance(d.attribute_value, str) and d.attribute_value.strip():
                    existing_values.add(d.attribute_value.lower())

            new_values_added = False
            for val in attribute_values:
                # Ensure we only compare lowercase strings
                if val and isinstance(val, str) and str(val).lower() not in existing_values:
                    attribute_doc.append("item_attribute_values", {"attribute_value": val, "abbr": val})
                    new_values_added = True
            if new_values_added:
                attribute_doc.save(ignore_permissions=True)
    except Exception as e:
        frappe.log_error(message=traceback.format_exc(), title=f"Error in _create_or_get_item_attribute for {attribute_name}")
        raise e

def create_variant_product_return(product, integration="shopify"):
    """
    Creates an Item Template and its variants in ERPNext.
    """
    title = product.get("title", "Untitled Product")
    frappe.log_error(f"Starting variant creation for: {title}", "Shopmate Import")
    variants = product.get("variants", [])
    if not variants:
        return {"error": f"Product '{product.get('title')}' is marked as having variants, but none are provided."}

    # --- 1. Collect all unique attribute values ---
    colors = list(set(v['color'] for v in variants if v.get('color')))
    sizes = list(set(v['size'] for v in variants if v.get('size')))

    # --- 2. Ensure Item Attributes exist ---
    if colors:
        frappe.log_error(f"Ensuring 'Color' attribute exists with values: {colors}", "Shopmate Import")
        _create_or_get_item_attribute("Color", colors)
    if sizes:
        frappe.log_error(f"Ensuring 'Size' attribute exists with values: {sizes}", "Shopmate Import")
        _create_or_get_item_attribute("Size", sizes)

    # --- 3. Create the Item Template using shopmate_id ---
    template_item_code = product.get("shopmate_id")
    if not template_item_code:
        return {"error": f"Product '{product.get('title')}' is missing shopmate_id."}

    template_attributes = []
    if colors:
        template_attributes.append({"attribute": "Color"})
    if sizes:
        template_attributes.append({"attribute": "Size"})

    if frappe.db.exists("Item", template_item_code):
        frappe.log_error(f"Template item {template_item_code} already exists. Fetching it.", "Shopmate Import")
        template_doc = frappe.get_doc("Item", template_item_code)
    else:
        frappe.log_error(f"Template item {template_item_code} does not exist. Creating it.", "Shopmate Import")
        template_fields = {
            "doctype": "Item",
            "name": template_item_code,
            "item_code": template_item_code,
            "item_group": "All Item Groups",
            "description": product.get("description", ""),
            "stock_uom": "Nos",
            "has_variants": 1,
            "variant_based_on": "Item Attribute",
            "attributes": template_attributes,
            "image": product.get("product_image_url"),
            "data_source": "Shopmate",
            "custom_ecommerce_vendor": product.get("shopmate_vendor")
        }
        template_doc = frappe.get_doc(template_fields)
        template_doc.item_name = product.get("title")
        template_doc.flags.from_integration = True
        template_doc.insert(ignore_permissions=True)
        frappe.db.commit()
    
    template_doc.item_code = template_doc.name
    frappe.log_error(f"Successfully created/found template item: {template_doc.name}", "Shopmate Import")
    
    # --- 4. Prepare for variant creation ---
    shopify_product_id = product.get("shopify_id")
    if not shopify_product_id:
        return {"error": f"Product '{title}' is missing shopify_id for variant creation."}
    product["id"] = shopify_product_id

    # Commit the template creation before proceeding to variants
    frappe.db.commit()
    
    # --- 5. Create each variant item and link it ---
    for variant in variants:
        item_code = variant.get("shopmate_id")
        if not item_code:
            frappe.log_error(f"Skipping variant for '{title}' because it is missing a shopmate_id.", "Shopmate Import")
            continue

        if frappe.db.exists("Item", item_code):
            continue # Skip if already exists

        variant_attributes = []
        if variant.get("color"):
            variant_attributes.append({"attribute": "Color", "attribute_value": variant.get("color")})
        if variant.get("size"):
            variant_attributes.append({"attribute": "Size", "attribute_value": variant.get("size")})

        item_fields = {
            "doctype": "Item",
            "name": item_code,
            "item_code": item_code,
            "item_name": variant.get("title"),
            "item_group": template_doc.item_group,
            "stock_uom": "Nos",
            "variant_of": template_doc.name,
            "attributes": variant_attributes,
            "image": variant.get("image") or product.get("product_image_url"),
            "valuation_rate": variant.get("cost_price", 0),
            "shopify_selling_rate": variant.get("sale_price", 0),
            "data_source": "Shopmate",
            "custom_ecommerce_vendor": product.get("shopmate_vendor")
        }

        try:
            item_doc = frappe.get_doc(item_fields)
            item_doc.flags.from_integration = True
            item_doc.insert(ignore_permissions=True)

            # --- Ecommerce Item for the variant ---
            ecommerce_fields = {
                "doctype": "Ecommerce Item",
                "erpnext_item_code": item_doc.name,
                "integration": integration,
                "integration_item_code": product.get("id"), # Shopify Product ID
                "variant_id": variant.get("shopify_id"),   # Shopify Variant ID
                "sku": variant.get("sku"),
                "item_name": variant.get("title"),
                "published": 1,
                "variant_of": template_doc.name
            }
            ecommerce_item = frappe.get_doc(ecommerce_fields)
            ecommerce_item.insert(ignore_permissions=True, ignore_if_duplicate=True)

            # --- Item Price for the variant ---
            sale_price = variant.get("sale_price")
            if sale_price is not None:
                if not frappe.db.exists("Item Price", {"item_code": item_doc.name, "price_list": "Standard Selling"}):
                    frappe.get_doc({
                        "doctype": "Item Price",
                        "item_code": item_doc.name,
                        "price_list": "Standard Selling",
                        "price_list_rate": sale_price,
                        "selling": 1,
                        "currency": frappe.db.get_single_value("Global Defaults", "default_currency") or "USD",
                    }).insert(ignore_permissions=True)

        except frappe.DuplicateEntryError:
            frappe.log_error(f"Variant item {item_code} already exists.", "Shopmate Import")
        except Exception as e:
            frappe.log_error(message=frappe.get_traceback(), title=f"Error creating variant {item_code} for {title}")

    return {"created": True, "item_code": template_doc.name}

def create_single_item_return(product, integration="shopify"):
    """
    Creates a single, non-variant item in ERPNext.
    """
    shopify_product_id = product.get("shopify_id")
    if not shopify_product_id:
        return {"error": "Shopify product ID is required for import."}
    product["id"] = shopify_product_id

    variants = product.get("variants", [])
    if not variants:
        return {"error": f"Product '{product.get('title')}' has no variants listed."}

    # For simple products, we only process the first variant
    variant = variants[0]
    created, updated = _create_standalone_item(product, variant, integration)

    if created:
        return {"created": True}
    elif updated:
        return {"updated": True}
    else:
        return {"info": "Processed"}

def _create_standalone_item(product, variant, integration):
    """
    Creates or updates a single Item doc (can be a standalone item or a variant).
    Returns a tuple of (created, updated) booleans.
    """
    created = False
    updated = False

    item_code = variant.get("shopmate_id")
    if not item_code:
        return created, updated

    # Check if an Item with this shopmate_id already exists
    if frappe.db.exists("Item", item_code):
        updated = True
        # For now, we skip updating existing items to prevent data loss.
        return created, updated

    # --- Item Group ---
    item_group = "All Item Groups"
    ai_category = product.get("ai_category_response")
    if ai_category:
        try:
            cat_dict = ast.literal_eval(ai_category)
            item_group = cat_dict.get("level_1") or item_group
        except Exception:
            pass
    if item_group and item_group != "All Item Groups":
        if not frappe.db.exists("Item Group", item_group):
            try:
                item_group_doc = frappe.get_doc({
                    "doctype": "Item Group",
                    "item_group_name": item_group,
                    "parent_item_group": "All Item Groups",
                    "is_group": 0
                })
                item_group_doc.insert(ignore_permissions=True)
            except Exception as e:
                frappe.log_error(f"Failed to create Item Group {item_group}: {e}")

    # --- Item Fields ---
    main_image = variant.get("image") or product.get("product_image_url")
    tags = []
    if product.get("is_alcohol_check_applied"):
        tags.append("Alcohol Check Applied")

    length = width = height = None
    if variant.get("dimensions"):
        try:
            dims = ast.literal_eval(variant["dimensions"]) if isinstance(variant["dimensions"], str) else variant["dimensions"]
            length = dims.get("length")
            width = dims.get("width")
            height = dims.get("height")
        except Exception:
            pass

    item_fields = {
        "doctype": "Item",
        "item_code": item_code,
        "name": item_code,
        "item_name": product.get("title"),
        "item_group": item_group,
        "description": product.get("description", ""),
        "stock_uom": "Nos",
        "disabled": 0,
        "image": main_image,
        "weight": variant.get("weight"),
        "volumentric_weight": variant.get("volumetric_weight"),
        "length": length,
        "width": width,
        "height": height,
        "tags": ", ".join(tags) if tags else None,
        "custom_last_sync_time": product.get("last_scrap_update"),
        "opening_stock": 0,
        "valuation_rate": variant.get("cost_price", 0),
        "shopify_selling_rate": variant.get("sale_price", 0),
        "data_source": "Shopmate",
        "custom_ecommerce_vendor": product.get("shopmate_vendor")
    }

    try:
        item_doc = frappe.get_doc(item_fields)
        item_doc.flags.from_integration = True
        item_doc.insert(ignore_permissions=True)
        created = True
    except frappe.DuplicateEntryError:
        item_doc = frappe.get_doc("Item", item_code)
        updated = True

    # --- Ecommerce Item ---
    ecommerce_fields = {
        "doctype": "Ecommerce Item",
        "erpnext_item_code": item_doc.name, # Use item_doc.name which is the item_code
        "integration": integration,
        "integration_item_code": product.get("id"),
        "variant_id": variant.get("shopify_id"),
        "sku": variant.get("sku"),
        "item_name": product.get("title"),
        "published": 1,
    }

    try:
        ecommerce_item = frappe.get_doc(ecommerce_fields)
        ecommerce_item.insert(ignore_permissions=True, ignore_if_duplicate=True)
    except frappe.DuplicateEntryError:
        pass

    # --- Item Price ---
    sale_price = variant.get("sale_price")
    if sale_price is not None:
        if not frappe.db.exists("Item Price", {"item_code": item_doc.name, "price_list": "Standard Selling"}):
            try:
                frappe.get_doc({
                    "doctype": "Item Price",
                    "item_code": item_doc.name,
                    "price_list": "Standard Selling",
                    "price_list_rate": sale_price,
                    "selling": 1,
                    "currency": frappe.db.get_single_value("Global Defaults", "default_currency") or "USD",
                }).insert(ignore_permissions=True)
            except Exception as e:
                frappe.log_error(f"Failed to create Item Price for {item_doc.name}: {e}")

    return created, updated
