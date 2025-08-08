import frappe
import json
import os
import ast
import time
from ecommerce_integrations.shopify.utils import create_shopify_log

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
    import requests
    import random
    shopify_product_id = product.get("shopify_id")
    if not shopify_product_id:
        frappe.throw("Shopify product ID is required for import.")
    product["id"] = shopify_product_id

    # Now create in ERPNext
    try:
        variants = product.get("variants", [])
        # Parse ai_category_response for item group
        item_group = "All Item Groups"
        ai_category = product.get("ai_category_response")
        if ai_category:
            try:
                cat_dict = ast.literal_eval(ai_category)
                item_group = cat_dict.get("level_1") or item_group
            except Exception:
                pass
        # Ensure item_group exists
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
        main_image = product.get("product_image_url")
        tags = []
        if product.get("is_alcohol_check_applied"):
            tags.append("Alcohol Check Applied")
        created = False
        updated = False
        for variant in variants:
            sku = variant.get("sku")
            if not sku:
                continue
            exists = frappe.db.exists("Ecommerce Item", {"sku": sku, "integration": integration})
            if exists:
                updated = True
                continue
            length = width = height = None
            if variant.get("dimensions"):
                try:
                    dims = ast.literal_eval(variant["dimensions"]) if isinstance(variant["dimensions"], str) else variant["dimensions"]
                    length = dims.get("length")
                    width = dims.get("width")
                    height = dims.get("height")
                except Exception:
                    pass
            last_scrap_update = product.get("last_scrap_update")
            item_code = sku
            item_fields = {
                "doctype": "Item",
                "name": item_code,
                "item_code": item_code,
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
                "custom_last_sync_time": last_scrap_update,
                "opening_stock": 0, # opening stock set to 0
                "valuation_rate": variant.get("cost_price", 0),
                "shopify_selling_rate": variant.get("sale_price", 0),
                "data_source": "Shopmate"
            }
            item_fields = {k: v for k, v in item_fields.items() if v is not None}
            try:
                item_doc = frappe.get_doc(item_fields)
                item_doc.flags.from_integration = True
                item_doc.insert(ignore_permissions=True, ignore_if_duplicate=True)
                created = True
            except frappe.DuplicateEntryError:
                item_doc = frappe.get_doc("Item", item_code)
                updated = True
            # Collect all changes before saving
            supplier = product.get("vendor")
            vendor_url = product.get("vendor_url")
            cost_price = variant.get("cost_price")
            if supplier:
                supplier_fields = {
                    "doctype": "Item Supplier",
                    "parenttype": "Item",
                    "parent": item_code,
                    "supplier": supplier,
                    "custom_product_url": vendor_url,
                    "custom_price": cost_price,
                    "main_vendor": 1,
                }
                supplier_fields = {k: v for k, v in supplier_fields.items() if v is not None}
                exists = frappe.db.exists("Item Supplier", {"parent": item_code, "supplier": supplier})
                if not exists:
                    item_doc.append("supplier_items", supplier_fields)
            # Barcode
            barcode = variant.get("barcode")
            barcode_type = None
            if barcode:
                barcode_row = {
                    "barcode": barcode,
                    "barcode_type": barcode_type,
                    "uom": "Nos"
                }
                if not any(b.barcode == barcode for b in getattr(item_doc, "barcodes", [])):
                    item_doc.append("barcodes", barcode_row)
            # Save only once after all changes
            try:
                item_doc.save(ignore_permissions=True)
            except Exception as e:
                frappe.log_error(f"Failed to save Item {item_code}: {e}")
            # Ecommerce Item
            ecommerce_fields = {
                "doctype": "Ecommerce Item",
                "erpnext_item_code": item_doc.item_code,
                "integration": integration,
                "integration_item_code": shopify_product_id,
                "variant_id": variant.get("id"),
                "sku": sku,
                "item_name": product.get("title"),
                "published": 1,
            }
            try:
                ecommerce_item = frappe.get_doc(ecommerce_fields)
                ecommerce_item.insert(ignore_permissions=True, ignore_if_duplicate=True)
            except frappe.DuplicateEntryError:
                pass
            # Item Price
            sale_price = variant.get("sale_price")
            if sale_price is not None:
                currency = frappe.db.get_single_value("Global Defaults", "default_currency") or "USD"
                item_price_fields = {
                    "doctype": "Item Price",
                    "item_code": item_doc.item_code,
                    "price_list": "Standard Selling",
                    "price_list_rate": sale_price,
                    "selling": 1,
                    "currency": currency,
                }
                exists = frappe.db.exists("Item Price", {
                    "item_code": item_doc.item_code,
                    "price_list": "Standard Selling"
                })
                if not exists:
                    try:
                        frappe.get_doc(item_price_fields).insert(ignore_permissions=True)
                    except Exception as e:
                        frappe.log_error(f"Failed to create Item Price for {item_doc.item_code}: {e}")
        if created:
            return {"created": True}
        elif updated:
            return {"updated": True}
        else:
            return {"info": "Processed"}
    except Exception as e:
        return {"error": f"ERPNext error: {str(e)}"}
