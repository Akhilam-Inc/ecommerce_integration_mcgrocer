import frappe
import requests
import json
import ast
import time

from ecommerce_integrations.shopify.constants import SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log, get_shopify_headers

def upload_metafield( product_id, key, value, type):
    setting = frappe.get_doc(SETTING_DOCTYPE)
    headers = get_shopify_headers(setting)
    api_version = "2024-10"
    url = f"https://{setting.shopify_url}/admin/api/{api_version}/products/{product_id}/metafields.json"

    payload = {
        "metafield": {
            "namespace": "custom",
            "key": key,
            "value": value,
            "type": type,
        }
    }
    response = requests.post(url, headers=headers, json=payload)
    
    try:
        response.raise_for_status()
        create_shopify_log(message="Successfully uploaded metafield", status="Success",request_data=payload, response_data=response.json())
    except requests.HTTPError as e:
        create_shopify_log(message=f"Error uploading metafield: {response.text}", status="Error", request_data=payload, response_data=response.json())
        return None
    
    return response.json()

def add_ai_summary(product_id, ai_summary):
    upload_metafield(product_id, "summary", ai_summary, "multi_line_text_field")

def add_ai_title(product_id, ai_title):
    upload_metafield(product_id, "ai_title", ai_title, "single_line_text_field")
