import requests
import frappe
import json

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
      create_shopify_log(message=f"Deleted Item [{item_id}]", status="Success", response_data=response.json())
      erpnext_item_name = frappe.get_value("Ecommerce Item", {"integration_item_code": item_id}, "erpnext_item_code")
      frappe.db.set_value("Item", erpnext_item_name, "deleted_from_shopify", 1)
      frappe.msgprint(f"Deleted Item [{item_id}] from Shopify")
            
    else:
      create_shopify_log(message=f"Failed to delete item [{item_id}]", status="Information", response_data=response.json())