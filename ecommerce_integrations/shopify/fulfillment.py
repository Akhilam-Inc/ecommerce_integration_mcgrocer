from copy import deepcopy
from datetime import datetime
import json
import requests
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
import shopify

from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.shopify.product import get_item_code


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")


def get_shopify_item_and_variant_id(erpnext_item_code):
    """
    Retrieve the Shopify item code and variant ID associated with a given ERPNext item code.

    Args:
        erpnext_item_code (str): The item code in ERPNext for which the Shopify integration details are required.

    Returns:
        tuple: A tuple containing the Shopify integration item code (str) and variant ID (str),
               or None if no matching record is found.
    """
    return frappe.db.get_value("Ecommerce Item", 
                               {"integration": "Shopify", "erpnext_item_code": erpnext_item_code}, 
                               ["integration_item_code", "variant_id"])


def get_fulfillment_items_from_dn(dn_items):
    """
    Generate a list of fulfillment items from delivery note items.

    Args:
        dn_items (list): A list of delivery note items, where each item is expected
                         to have attributes `item_code` and `qty`.

    Returns:
        list: A list of dictionaries, where each dictionary represents a fulfillment
              item with the following keys:
              - "id" (str): The Shopify item ID.
              - "variant_id" (str): The Shopify variant ID.
              - "quantity" (int/float): The quantity of the item.

    Notes:
        - The function uses `get_shopify_item_and_variant_id` to retrieve the Shopify
          item ID and variant ID for each delivery note item.
        - Items without a valid Shopify item ID are excluded from the result.
    """
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


def get_order_fullfilments_orders(shopify_order_id, setting):
    """
    Fetches the fulfillment orders for a given Shopify order ID.

    Args:
        shopify_order_id (str): The ID of the Shopify order for which fulfillment orders are to be retrieved.
        setting (object): An object containing Shopify configuration, including the shopify_url.

    Returns:
        dict: A dictionary containing the JSON response from the Shopify API, which includes the fulfillment orders.

    Raises:
        requests.exceptions.RequestException: If the HTTP request to the Shopify API fails.
    """
    fulfillment_order_url = f"https://{setting.shopify_url}/admin/api/2024-10/orders/{shopify_order_id}/fulfillment_orders.json"
    headers = get_shopify_headers(setting)
    response = requests.get(fulfillment_order_url, headers=headers, timeout=10)
    return response.json()


def get_shopify_headers(setting):
    """
    Generate headers required for Shopify API requests.

    Args:
        setting (object): An object that provides access to configuration settings,
                          including a method `get_password` to retrieve the Shopify
                          access token.

    Returns:
        dict: A dictionary containing the headers:
              - "Content-Type": Specifies the media type as JSON.
              - "Accept": Specifies the expected response format as JSON.
              - "X-Shopify-Access-Token": The access token for authenticating with Shopify.
    """
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Access-Token": setting.get_password("password")
    }


def create_fulfillment_for_dn_items(fulfillment_order_id, line_items, setting):
    """
    Create a fulfillment for delivery note items in Shopify.
    This function sends a POST request to the Shopify API to create a fulfillment
    for the specified fulfillment order ID and line items. It uses the provided
    Shopify settings to authenticate the request.
    Args:
        fulfillment_order_id (str): The ID of the fulfillment order in Shopify.
        line_items (list): A list of line items to be fulfilled, where each item
            should be a dictionary containing the necessary details for fulfillment.
        setting (object): An object containing Shopify settings, including the
            `shopify_url` and authentication details.
    Returns:
        requests.Response: The response object from the Shopify API request.
    """
    fulfillment_url = f"https://{setting.shopify_url}/admin/api/2025-01/fulfillments.json"
    headers = get_shopify_headers(setting)
    payload = {
        "fulfillment": {
            "message": "Fulfillment created from ERPNext",
            "line_items_by_fulfillment_order": [
                {
                    "fulfillment_order_id": fulfillment_order_id,
                    "fulfillment_order_line_items": line_items,
                }
            ]
        }
    }
    
    return requests.post(fulfillment_url, json=payload, headers=headers, timeout=10)


def create_shopify_fulfillment(delivery_note_doc, setting):
    if not cint(setting.sync_erpnext_fulfillment):
        frappe.throw("Syncing of ERPNext delivery note as fulfillment is disabled in Shopify settings.")

    session = shopify.Session(setting.shopify_url, "2023-04", setting.get_password("password"))
    shopify.ShopifyResource.activate_session(session)

    try:
        fulfillment_orders = get_order_fullfilments_orders(delivery_note_doc.shopify_order_id, setting)
        if not fulfillment_orders['fulfillment_orders']:
            frappe.throw(f"No fulfillment orders found for order {delivery_note_doc.shopify_order_id}")
        fulfillment_order = fulfillment_orders['fulfillment_orders'][-1]

        delivery_items = get_fulfillment_items_from_dn(delivery_note_doc.items)
        items_to_fulfill = [
            {"id": line_item['id'], "quantity": int(item['quantity'])}
            for line_item in fulfillment_order['line_items']
            for item in delivery_items
            if str(line_item['variant_id']) == str(item['variant_id'])
        ]

        create_shopify_log(message=f"Creating fulfillment for delivery note {delivery_note_doc.name}", status="Information", request_data={
            "fulfillment_order": fulfillment_order,
            "items_to_fulfill": items_to_fulfill
        })
        response = create_fulfillment_for_dn_items(fulfillment_order['id'], items_to_fulfill, setting)

        if response.status_code in (200, 201):
            fulfillment_data = response.json()["fulfillment"]
            fulfillment_id = str(fulfillment_data["id"])
            delivery_note_doc.db_set(FULLFILLMENT_ID_FIELD, fulfillment_id)
            delivery_note_doc.add_comment(text=f"Fulfillment created in Shopify: {fulfillment_id}")
            create_shopify_log(status="Success", message=f"Fulfillment created in Shopify: {fulfillment_id}", request_data=json.dumps(response.json() or {}))
        elif response.status_code == 422:
            create_shopify_log(status="Error", message="Fulfillment creation failed. The fulfillment order is already fulfilled in Shopify.", request_data=json.dumps(response.json() or {}), response_data=response.text)
            return
        else:
            response_content = None
            try:
                response_content = response.json()
            except ValueError:
                response_content = response.text or "No content"

            if isinstance(response_content, dict):
                response_content = {k: v for k, v in response_content.items() if not callable(v)}

            response_headers = response.headers

            create_shopify_log(
                status="Invalid", 
                message=f"Failed to create fulfillment in Shopify: {response.status_code} - {response.reason}", 
                response_data=f"Content: {str(response_content)}, Headers: {dict(response_headers)}", 
                rollback=True
            )
           
            frappe.throw(f"Failed to create fulfillment in Shopify: {response.status_code} - {response.reason}")
    except (ValueError, KeyError, requests.RequestException, shopify.ApiAccessError) as e:
        frappe.throw(str(e))

def prepare_shopify_fulfillment(delivery_note_doc):
    """
    Prepares and processes the fulfillment of a Shopify order based on the given delivery note document.

    This function performs the following steps:
    1. Sets the current user to "Administrator".
    2. Retrieves the Shopify settings document.
    3. Checks if the delivery note document has a Shopify order ID.
    4. If a Shopify order ID exists:
        - Creates a Shopify fulfillment for the order.
        - Logs the success of the fulfillment process.
        - Displays a success message to the user.
    5. If no Shopify order ID exists, raises an exception.
    6. Handles any exceptions during the process by logging the error.

    Args:
        delivery_note_doc (Document): The delivery note document containing details of the order to be fulfilled.

    Raises:
        frappe.exceptions.ValidationError: If the delivery note does not have a Shopify order ID.
        Exception: For any other errors encountered during the fulfillment process.
    """
    frappe.set_user("Administrator")
    setting = frappe.get_doc(SETTING_DOCTYPE)

    try:
        if delivery_note_doc.shopify_order_id:
            create_shopify_fulfillment(delivery_note_doc, setting)
            create_shopify_log(
                status="Success", 
                message=f"Delivery note [{delivery_note_doc.name}] has fulfilled Shopify order [{delivery_note_doc.shopify_order_id}].",
                request_data=json.dumps(delivery_note_doc.as_dict(), default=json_serial)
            )
            frappe.msgprint(f"Order [{delivery_note_doc.shopify_order_id}] has been marked as fulfilled in Shopify.", alert=True, indicator="green")
        else:
            frappe.throw("The delivery note does not have a Shopify order ID.")
    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)


def prepare_delivery_note(payload, request_id=None):
    """
    Prepares a delivery note for a Shopify order by syncing it with the system.

    This function sets the user to "Administrator", retrieves the sales order
    associated with the given Shopify order ID, and creates a delivery note if
    the sales order exists. It also logs the status of the operation.

    Args:
        payload (dict): The Shopify order payload containing order details.
        request_id (str, optional): An optional request ID for tracking purposes.

    Raises:
        Exception: Logs an error and performs a rollback if an exception occurs during processing.

    Side Effects:
        - Sets the current user to "Administrator".
        - Sets the `frappe.flags.request_id` to the provided request ID.
        - Creates a delivery note if the sales order is found.
        - Logs the operation status (Success, Invalid, or Error).
    """
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
    """
    Creates a Delivery Note in the system based on the Shopify order and settings.

    Args:
        shopify_order (dict): The Shopify order data containing details such as fulfillments, order ID, and notes.
        setting (object): The settings object containing configuration for syncing delivery notes and naming series.
        so (object): The Sales Order object associated with the Shopify order.

    Returns:
        None: The function does not return any value. It creates and submits a Delivery Note if conditions are met.

    Notes:
        - The function checks if syncing delivery notes is enabled in the settings.
        - It iterates through the fulfillments in the Shopify order and creates a Delivery Note for each fulfillment
          that does not already exist in the system.
        - The Delivery Note is populated with relevant data from the Shopify order and fulfillment, including
          order ID, order number, fulfillment ID, posting date, and items.
        - If the Shopify order contains a note, it is added as a comment to the Delivery Note.
    """
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
    """
    Matches delivery note items with Shopify fulfillment items and updates their quantities 
    and warehouse information based on the integration settings.

    Args:
        dn_items (list): A list of delivery note items, where each item is a dictionary 
                         containing details such as `item_code`.
        fulfillment_items (list): A list of Shopify fulfillment items, where each item is 
                                  a dictionary containing details such as `quantity`.
        location_id (str, optional): The Shopify location ID used to determine the warehouse. 
                                     Defaults to None.

    Returns:
        list: A list of updated delivery note items with matched quantities and warehouse 
              information.
    """

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


def cancel_fulilment(delivery_note_doc, setting):
    """
    TODO:
    Cancels a fulfillment in Shopify based on the provided delivery note document
    and Shopify settings.

    Args:
        delivery_note_doc (Document): The delivery note document to be canceled.
        setting (Document): The Shopify settings document containing configuration.

    Raises:
        frappe.ValidationError: If syncing of ERPNext delivery note as fulfillment
                                is disabled in Shopify settings.
    """
    if not cint(setting.sync_erpnext_fulfillment):
        frappe.throw("Syncing of ERPNext delivery note as fulfillment is disabled in Shopify settings.")
