from copy import deepcopy
import json

import frappe
from erpnext.selling.doctype.sales_order.sales_order import make_delivery_note
from frappe.utils import cint, cstr, getdate

from ecommerce_integrations.shopify.constants import (
    FULLFILLMENT_ID_FIELD,
    ORDER_ID_FIELD,
    ORDER_NUMBER_FIELD,
    SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.order import get_sales_order
from ecommerce_integrations.shopify.utils import create_shopify_log, json_serializer
import shopify


def prepare_delivery_note(payload, request_id=None):
    frappe.set_user("Administrator")
    setting = frappe.get_doc(SETTING_DOCTYPE)
    frappe.flags.request_id = request_id

    order = payload

    try:
        sales_order = get_sales_order(cstr(order["id"]))
        if sales_order:
            create_delivery_note(order, setting, sales_order)
            create_shopify_log(status="Success")
        else:
            create_shopify_log(status="Invalid", message="Sales Order not found for syncing delivery note.")
    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)


def create_delivery_note(shopify_order, setting, so):
    if not cint(setting.sync_delivery_note):
        return

    for fulfillment in shopify_order.get("fulfillments"):
        if (
            not frappe.db.get_value("Delivery Note", {FULLFILLMENT_ID_FIELD: fulfillment.get("id")}, "name")
            and so.docstatus == 1
        ):

            dn = make_delivery_note(so.name)
            setattr(dn, ORDER_ID_FIELD, fulfillment.get("order_id"))
            setattr(dn, ORDER_NUMBER_FIELD, shopify_order.get("name"))
            setattr(dn, FULLFILLMENT_ID_FIELD, fulfillment.get("id"))
            dn.set_posting_time = 1
            dn.posting_date = getdate(fulfillment.get("created_at"))
            dn.naming_series = setting.delivery_note_series or "DN-Shopify-"
            dn.items = get_fulfillment_items(
                dn.items, fulfillment.get("line_items"), fulfillment.get("location_id")
            )
            dn.flags.ignore_mandatory = True
            dn.save()
            dn.submit()

            if shopify_order.get("note"):
                dn.add_comment(text=f"Order Note: {shopify_order.get('note')}")


def get_fulfillment_items(dn_items, fulfillment_items, location_id=None):
    # local import to avoid circular imports
    from ecommerce_integrations.shopify.product import get_item_code

    fulfillment_items = deepcopy(fulfillment_items)

    setting = frappe.get_cached_doc(SETTING_DOCTYPE)
    wh_map = setting.get_integration_to_erpnext_wh_mapping()
    warehouse = wh_map.get(str(location_id)) or setting.warehouse

    final_items = []

    def find_matching_fullfilement_item(dn_item):
        nonlocal fulfillment_items

        for item in fulfillment_items:
            if get_item_code(item) == dn_item.item_code:
                fulfillment_items.remove(item)
                return item

    for dn_item in dn_items:
        if shopify_item := find_matching_fullfilement_item(dn_item):
            final_items.append(
                dn_item.update({"qty": shopify_item.get("quantity"), "warehouse": warehouse})
            )

    return final_items


def prepare_shopify_fulfillment(delivery_note_doc):
    frappe.set_user("Administrator")
    setting = frappe.get_doc(SETTING_DOCTYPE)

    try:
        if delivery_note_doc.shopify_order_id:
            create_shopify_fulfillment(delivery_note_doc, setting)
            create_shopify_log(status="Success", message=f"Delivery note [{delivery_note_doc.name}] has fulfilled Shopify order [{delivery_note_doc.shopify_order_id}].")
            frappe.msgprint(f"Order [{delivery_note_doc.shopify_order_id}] has been marked as fulfilled in Shopify.")
        else:
            frappe.throw("The delivery note does not have a Shopify order ID.")
    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)


def get_order_fullfilments_orders(shopify_order_id, setting):
    fulfillment_order_url = f"https://{setting.shopify_url}/admin/api/2024-10/orders/{shopify_order_id}/fulfillment_orders.json"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Access-Token": setting.get_password("password")
    }
    import requests
    response = requests.get(fulfillment_order_url, headers=headers)
    return response.json()

    return None
def create_shopify_fulfillment(delivery_note_doc, setting):
    if not cint(setting.sync_erpnext_fulfillment):
        frappe.throw(f"Syncing of ERPNext delivery note as fulfillment is disabled in Shopify settings.")

    # Use a valid API version (check Shopify's documentation for the latest stable version)
    session = shopify.Session(setting.shopify_url, "2023-04", setting.get_password("password"))
    shopify.ShopifyResource.activate_session(session)

    try:
        fulfillment_orders = get_order_fullfilments_orders(delivery_note_doc.shopify_order_id, setting)
        if not fulfillment_orders['fulfillment_orders']:
            frappe.throw(f"No fulfillment orders found for order {delivery_note_doc.shopify_order_id}")
        fulfillment_order = fulfillment_orders['fulfillment_orders'][len(fulfillment_orders['fulfillment_orders']) - 1]
        
        # Filter fulfillment items to match those in the delivery note
        delivery_items = get_fulfillment_items_from_dn(delivery_note_doc.items)
        items_to_fulfill = []

        for line_item in fulfillment_order['line_items']:
            for item in delivery_items:
                if str(line_item['variant_id']) == str(item['variant_id']):
                    items_to_fulfill.append({
                        "id": line_item['id'],
                        "quantity": line_item['quantity'],
                    })
        # frappe.throw(f"{delivery_items} \nItems to fulfill: {fulfillment_orders}")
        # frappe.throw(f"To Fulfill: {items_to_fulfill}")
        # response = create_fulfillment_for_fulfillment_order(fulfillment_order['id'], setting)
        response = create_fulfillment_for_dn_items(fulfillment_order['id'], items_to_fulfill, setting)
       
        if response.status_code in (200, 201):
            fulfillment_data = response.json()["fulfillment"]
            fulfillment_id = str(fulfillment_data["id"])
            delivery_note_doc.db_set(FULLFILLMENT_ID_FIELD, fulfillment_id)
            delivery_note_doc.add_comment(text=f"Fulfillment created in Shopify: {fulfillment_id}")
            create_shopify_log(status="Success", message=f"Fulfillment created in Shopify: {fulfillment_id}", request_data=json.dumps(response.json() or {}))
        elif response.status_code == 422:
            create_shopify_log(status="Error", message=f"Fulfillment creation failed. The fulfillment order is already fulfilled in Shopify.", request_data=json.dumps(response.json() or {}), response_data=response.text)
            return
        else:
            # Safely get response content without assuming it's JSON
            response_content = None
            try:
                response_content = response.json()
            except ValueError:
                response_content = response.text or "No content"

            # Ensure response_content is serializable
            if isinstance(response_content, dict):
                response_content = {k: v for k, v in response_content.items() if not callable(v)}

            # Include headers in the error message
            response_headers = response.headers

            create_shopify_log(
                status="Invalid", 
                message=f"Failed to create fulfillment in Shopify: {response.status_code} - {response.reason}", 
                response_data=f"Content: {str(response_content)}, Headers: {dict(response_headers)}",  # Ensure it's a string
                rollback=True
            )
            
            frappe.throw(f"Failed to create fulfillment in Shopify: {response.status_code} - {response.reason}")
    except Exception as e:
        frappe.throw(str(e))

def create_fulfillment_for_fulfillment_order(fulfillment_order_id, setting):
    fulfillment_url = f"https://{setting.shopify_url}/admin/api/2025-01/fulfillments.json"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Access-Token": setting.get_password("password")
    }

    # shiiping_companny = frappe.get_value("Shipping Rule", {"custom_display_name": delivery_note_doc.shipping_rule}, "shipping_company")
    payload = {
        
        "fulfillment":
        {
            "message":"Fulfillment created from ERPNext",
            "line_items_by_fulfillment_order":
            [
                {"fulfillment_order_id":fulfillment_order_id}
            ]
        }
    }

    import requests
    return requests.post(fulfillment_url, json=payload, headers=headers)

def cancel_fulilment(delivery_note_doc, setting):
    if not cint(setting.sync_erpnext_fulfillment):
        frappe.throw(f"Syncing of ERPNext delivery note as fulfillment is disabled in Shopify settings.")
    
    

def create_fulfillment_for_dn_items(fulfillment_order_id, line_items, setting):
    fulfillment_url = f"https://{setting.shopify_url}/admin/api/2025-01/fulfillments.json"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Access-Token": setting.get_password("password")
    }
    payload = {
        "fulfillment": {
            "message":"Fulfillment created from ERPNext",
             "line_items_by_fulfillment_order": [
                {
                    "fulfillment_order_id":fulfillment_order_id,
                    "fulfillment_order_line_items": line_items,
                }
            ]
        }
    }

    import requests
    return requests.post(fulfillment_url, json=payload, headers=headers)

def get_fulfillment_items_from_dn(dn_items):

    fulfillment_items = []

    for dn_item in dn_items:
        shopify_item_id, variant_id = get_shopify_item_and_variant_id(dn_item.item_code)
        if shopify_item_id:
            fulfillment_items.append({
                "id": shopify_item_id,
                "variant_id": variant_id,
                "quantity": dn_item.qty
            })

    return fulfillment_items

def get_shopify_item_and_variant_id(erpnext_item_code):
    return frappe.db.get_value("Ecommerce Item", 
                                          {"integration": "Shopify", "erpnext_item_code": erpnext_item_code}, 
                                          ["integration_item_code", "variant_id"])
