import requests
import frappe
import json
import time
import os
import ast

from ecommerce_integrations.shopify.utils import create_shopify_log

@frappe.whitelist()
def delete_items_from_shopify(erpnext_item_names):
  erpnext_item_names = json.loads(erpnext_item_names)
  shopify_item_ids = frappe.get_all(
    "Ecommerce Item",
    filters={
      "erpnext_item_code": ["in", erpnext_item_names],
      "integration": "shopify",
      },
    fields=["integration_item_code"],
  )

  # delete shopify items based on shopify item ids
  setting = frappe.get_doc("Shopify Setting")
  shopify_url = setting.shopify_url

  headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Shopify-Access-Token": setting.get_password("password")
  }

  for item_id in shopify_item_ids:
    item_id = item_id.integration_item_code
    url = f"https://{shopify_url}/admin/api/2025-01/products/{item_id}.json"
    response = requests.delete(url, headers=headers, timeout=10)

    if response.status_code == 200:
      create_shopify_log(
          message=f"Deleted Item [{item_id}]", status="Success", response_data=response.json())
      erpnext_item_name = frappe.get_value(
          "Ecommerce Item", {"integration_item_code": item_id}, "erpnext_item_code")
      frappe.db.set_value("Item", erpnext_item_name, "deleted_from_shopify", 1)
      frappe.msgprint(f"Deleted Item [{item_id}] from Shopify")

    else:
      create_shopify_log(
          message=f"Failed to delete item [{item_id}]", status="Information", response_data=response.json())


def create_shopify_product(product):
    setting = frappe.get_doc("Shopify Setting")
    shopify_url = setting.shopify_url
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Access-Token": setting.get_password("password")
    }
    payload = {
        "product": {
            "title": product["title"],
            "body_html": product.get("description", ""),
            "vendor": product.get("vendor", ""),
            "status": product.get("status", "active"),
            "images": [{"src": img["image_url"]} for img in product.get("images", [])],
            "variants": [
                {
                    "title": v.get("title", "Default Title"),
                    "sku": v.get("sku", ""),
                    "price": v.get("sale_price", 0),
                    "weight": v.get("weight", 0),
                    "weight_unit": "kg",
                    "inventory_quantity": v.get("stock", 0),
                    "barcode": v.get("barcode", None),
                } for v in product.get("variants", [])
            ]
        }
    }
    url = f"https://{shopify_url}/admin/api/2025-04/products.json"
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    if resp.status_code == 201:
        return resp.json()["product"]
    else:
        print(f"Failed to create product: {product['title']}")
        print(resp.text)
        return None

def import_production_items():
    json_path = os.path.join(frappe.get_site_path("private", "files"), "products.json")
    with open(json_path) as f:
        products = json.load(f)

    for product in products:
        # Remove variants without SKU
        if "variants" in product and isinstance(product["variants"], list):
            product["variants"] = [v for v in product["variants"] if v is not None and v.get("sku")]
        else:
            product["variants"] = []

        shopify_product = create_shopify_product(product)
        if shopify_product and shopify_product.get("variants") and len(shopify_product["variants"]) > 0:
            product["id"] = shopify_product.get("id")
            if product["variants"] and len(product["variants"]) > 0:
                product["variants"][0]["id"] = shopify_product["variants"][0].get("id")
        else:
            product["id"] = shopify_product.get("id") if shopify_product else None

        if shopify_product:
            print(f"Uploaded to Shopify: {product['title']}")
            try:
                create_item_and_ecommerce_item(product, integration="shopify")
                print(f"Created/Updated ERPNext Item and Ecommerce Item for: {product['title']}")
            except Exception as e:
                print(f"Failed to create ERPNext Item for {product['title']}: {e}")
        else:
            print(f"Failed to upload to Shopify: {product['title']}")
            continue

        time.sleep(0.5)

    updated_json_path = os.path.join(frappe.get_site_path("private", "files"), "products_updated.json")
    with open(updated_json_path, "w") as f:
        json.dump(products, f, indent=2)

def create_item_and_ecommerce_item(product, integration="shopify"):
    """
    For each variant, create or update an Item and Ecommerce Item in ERPNext,
    mapping all relevant fields and custom fields.
    """

    shopify_product_id = product.get("id")
    variants = product.get("variants", [])
    # Parse ai_category_response for item group
    item_group = "All Item Groups"
    ai_category = product.get("ai_category_response")
    if ai_category:
        try:
            # ai_category_response is a stringified dict
            cat_dict = ast.literal_eval(ai_category)
            item_group = cat_dict.get("level_1") or item_group
        except Exception:
            pass
    

    # Prepare main image
    main_image = product.get("product_image_url")
    # Prepare tags
    tags = []
    if product.get("is_alcohol_check_applied"):
        tags.append("Alcohol Check Applied")

    for variant in variants:
        sku = variant.get("sku")
        if not sku:
            continue

        # Dimensions
        length = width = height = None
        if variant.get("dimensions"):
            try:
                dims = ast.literal_eval(variant["dimensions"]) if isinstance(variant["dimensions"], str) else variant["dimensions"]
                length = dims.get("length")
                width = dims.get("width")
                height = dims.get("height")
            except Exception:
                pass

        # Barcode and barcode type
        barcode = variant.get("barcode")
        barcode_type = None
        if barcode:
            blen = len(str(barcode))
            # Simple mapping based on length
            barcode_type_map = {
                8: "EAN-8",
                12: "EAN-12",
                13: "EAN",
                10: "ISBN-10",
                14: "GTIN"
            }
            barcode_type = barcode_type_map.get(blen, "EAN")
        last_scrap_update = product.get("last_scrap_update")
        item_code = sku
        
        # Ensure item_group exists, otherwise create it as a child of 'All Item Groups'
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
                    
        item_fields = {
            "doctype": "Item",
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
            "opening_stock": variant.get("stock", 0),
            "valuation_rate": variant.get("cost_price", 0),
            "shopify_selling_rate": variant.get("sale_price", 0)
        }
        # Remove None values
        item_fields = {k: v for k, v in item_fields.items() if v is not None}

        try:
            item_doc = frappe.get_doc(item_fields)
            item_doc.insert(ignore_permissions=True, ignore_if_duplicate=True)
        except frappe.DuplicateEntryError:
            item_doc = frappe.get_doc("Item", item_code)

        # Create or update Item Supplier
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
            }
            # Remove None values
            supplier_fields = {k: v for k, v in supplier_fields.items() if v is not None}
            
            try:
                # Check if supplier already exists for this item
                exists = frappe.db.exists("Item Supplier", {"parent": item_code, "supplier": supplier})
                if not exists:
                    item_doc.append("supplier_items", supplier_fields)
                    item_doc.save(ignore_permissions=True)
            except Exception:
                pass

        # Create or update Ecommerce Item for this variant
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

        if barcode:
            barcode_row = {
                "barcode": barcode,
                "barcode_type": barcode_type,
                "uom": "Nos"
            }
            
            # Check for duplicates before appending
            if not any(b.barcode == barcode for b in getattr(item_doc, "barcodes", [])):
                item_doc.append("barcodes", barcode_row)
                item_doc.save(ignore_permissions=True)

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
        
        # Avoid duplicate Item Price
        exists = frappe.db.exists("Item Price", {
            "item_code": item_doc.item_code,
            "price_list": "Standard Selling"
        })
        if not exists:
            try:
                frappe.get_doc(item_price_fields).insert(ignore_permissions=True)
            except Exception as e:
                frappe.log_error(f"Failed to create Item Price for {item_doc.item_code}: {e}")

