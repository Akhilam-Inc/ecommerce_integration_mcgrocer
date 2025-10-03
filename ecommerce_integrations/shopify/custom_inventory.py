import frappe
import requests
import json
from ecommerce_integrations.shopify.constants import SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log


API_VERSION = "2024-10"
BASE_URL = None
HEADERS = None

def _init_config():
    """
    Initializes global BASE_URL and HEADERS using Frappe settings. 
    This function should only be called if the globals are currently None.
    """
    global BASE_URL
    global HEADERS

    try:
        # --- START CONFIGURATION PATTERN (Requires Frappe context) ---
        import frappe
        from ecommerce_integrations.shopify.utils import get_shopify_headers

        setting = frappe.get_doc(SETTING_DOCTYPE)
        headers = get_shopify_headers(setting)
        HEADERS = headers 
        BASE_URL = f"https://{setting.shopify_url}/admin/api/{API_VERSION}"
        return True

    except Exception as e:
        return False

def get_inventory_item_id(variant_id):
    """
    Fetches the inventory_item_id associated with the Product Variant ID.
    """

    url = f"{BASE_URL}/variants/{variant_id}.json"
    create_shopify_log(message=f"Fetching Inventory Item ID for Variant ID: {variant_id}...", status="Info", request_data=url)
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        inventory_item_id = data.get('variant', {}).get('inventory_item_id')
        
        if inventory_item_id:
            create_shopify_log(status="Success", message=f"Inventory Item ID found: {inventory_item_id}")
            return inventory_item_id
        else:
            create_shopify_log(status="Error", message=f"Variant {variant_id} or its inventory_item_id not found in response.")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"-> API Error during variant lookup: {e}")
        return None

def get_first_location_id():
    """
    Fetches the ID of the first active warehouse/location.
    """
        
    url = f"{BASE_URL}/locations.json"
    create_shopify_log(message=f"Fetching the first available Location ID...", status="Info", request_data=url)

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        locations = data.get('locations', [])
        
        if locations:
            location_id = locations[0].get('id')
            location_name = locations[0].get('name')
            print(f"-> Success. Location ID found: {location_id} ({location_name})")
            return location_id
        else:
            print("-> Error: No active locations found in the store.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"-> API Error during location lookup: {e}")
        return None

def set_inventory_level(inventory_item_id, location_id, new_quantity):
    """
    Sets the inventory level for the item at the specific location.
    """

    url = f"{BASE_URL}/inventory_levels/set.json"
    print(f"Setting Inventory Level for Item {inventory_item_id} at Location {location_id} to {new_quantity}...")

    payload = {
        "inventory_item_id": inventory_item_id,
        "location_id": location_id,
        "available": new_quantity
    }
    
    try:
        response = requests.post(url, headers=HEADERS, data=json.dumps(payload))
        response.raise_for_status()
        
        create_shopify_log(message=f"-> Success! Inventory updated.", status="Success", request_data=payload, response_data=response.json())
        return True

    except requests.exceptions.HTTPError as err:
        create_shopify_log(message=f"-> API Error setting inventory: {err}", status="Error", request_data=payload, response_data=response.json())
        return False
    except requests.exceptions.RequestException as e:
        frappe.log_error(f"-> Connection Error: {e}")
        return False
        
def update_inventory(variant_id, inventory_level):
    """
    The main callable function to update Shopify inventory for a given variant ID
    and desired stock level. It ensures configuration is initialized first.
    """

    if not BASE_URL or not HEADERS:
        if not _init_config():
            return False
        
    inventory_item_id = get_inventory_item_id(variant_id)
    if not inventory_item_id: return False
    #location_id = get_first_location_id() 
    #if not location_id: return False
    # return set_inventory_level(inventory_item_id, location_id, inventory_level)
    return set_inventory_level(inventory_item_id, "102069469449", inventory_level)
