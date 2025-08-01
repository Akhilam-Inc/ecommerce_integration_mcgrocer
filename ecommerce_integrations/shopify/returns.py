from ecommerce_integrations.shopify.constants import SETTING_DOCTYPE
import frappe
import requests
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.shopify.connection import temp_shopify_session

setting = frappe.get_doc(SETTING_DOCTYPE)


def fetch_open_returns_graphql(shopify_order_id):
    """
    Fetch open returns for a Shopify order using GraphQL API.
    """
    setting.get_password("password")
    url = f"https://{setting.shopify_url}/admin/api/2025-04/graphql.json"
    headers = {
        "X-Shopify-Access-Token": setting.get_password('password'),
        "Content-Type": "application/json"
    }
    query = """
    query ($orderId: ID!) {
      order(id: $orderId) {
        returns(first: 10, reverse: true) {
          edges {
            node {
              id
              status
              returnLineItems(first: 50) {
                edges {
                  node {
                    id
                    quantity
                    returnReason
                    returnableLineItem {
                      id
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    # Shopify order GID format: gid://shopify/Order/ORDER_ID
    variables = {"orderId": f"gid://shopify/Order/{shopify_order_id}"}
    response = requests.post(url, headers=headers, json={
                             "query": query, "variables": variables})
    response.raise_for_status()
    data = response.json()
    create_shopify_log(
        status="Success",
        message="Fetched returns from Shopify",
        response_data=f"#{url}\n #{variables} \n {data}")
    returns = []
    order = data.get("data", {}).get("order")
    if order and order.get("returns"):
        for edge in order["returns"]["edges"]:
            node = edge["node"]
            if node["status"] == "OPEN":
                return_line_items = []
                for li_edge in node["returnLineItems"]["edges"]:
                    li_node = li_edge["node"]
                    return_line_items.append({
                        # Use the original order line item ID for mapping
                        "line_item_id": li_node["returnableLineItem"]["id"].split("/")[-1] if li_node.get("returnableLineItem") else None,
                        "quantity": li_node["quantity"],
                        "return_reason": li_node.get("returnReason")
                    })
                returns.append({"return_line_items": return_line_items})
    return returns


@frappe.whitelist()
@temp_shopify_session
def sync_open_shopify_returns():
    """
    Fetch open customer returns from Shopify and update ERPNext Delivery Notes.
    """
    # Fetch open returns from Shopify (status: not closed/cancelled)
    # Adjust filter as per Shopify API
    open_returns = Return.find(status="open")
    create_shopify_log(
        status="Success",
        message="Fetched open returns from Shopify",
        response_data=open_returns
    )

    for shopify_return in open_returns:
        order_id = getattr(shopify_return, "order_id", None)
        return_line_items = getattr(shopify_return, "return_line_items", [])
        if not order_id or not return_line_items:
            continue

        # Find Delivery Note mapped to this Shopify order
        delivery_notes = frappe.get_all(
            "Delivery Note",
            filters={"shopify_order_id": str(order_id)},
            fields=["name"]
        )
        if not delivery_notes:
            continue

        dn_doc = frappe.get_doc("Delivery Note", delivery_notes[0]["name"])

        # Map return items by Shopify line item id
        return_items_map = {
            str(item["line_item_id"]): item for item in return_line_items}

        updated = False
        for item in dn_doc.items:
            shopify_line_item_id = getattr(item, "shopify_line_item_id", None)
            if shopify_line_item_id and str(shopify_line_item_id) in return_items_map:
                return_qty = return_items_map[str(
                    shopify_line_item_id)].get("quantity", 0)
                if return_qty > 0:
                    item.expected_return_qty = return_qty
                    updated = True

        # Indicate return requested
        dn_doc.return_requested = 1

        if updated:
            dn_doc.return_requested = 1
            dn_doc.save(ignore_permissions=True, ignore_validate_update_after_submit=True)
            frappe.db.commit()


@frappe.whitelist()
@temp_shopify_session
def sync_shopify_returns_for_delivery_note(delivery_note_name):
    """
    Update the Shopify Order related to delivery note using custom shopify_order_id the idea is to trigger order/updated webhook which will then be handled by handle_shopify_return function
    """
    delivery_note = frappe.get_doc("Delivery Note", delivery_note_name)
    shopify_order_id = delivery_note.shopify_order_id
    if not shopify_order_id:
        create_shopify_log(
            status="Error",
            message=f"Delivery Note {delivery_note_name} does not have a Shopify Order ID.",
            response_data=f"#{delivery_note_name}"
        )
        return

    url = f"https://{setting.shopify_url}/admin/api/2025-04/orders/{shopify_order_id}.json"
    headers = {
        "X-Shopify-Access-Token": setting.get_password('password'),
        "Content-Type": "application/json"
    }
    payload = {
        "order": {
            "id": shopify_order_id,
            "note": f"Updated at {frappe.utils.now()}"
        }
    }
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    create_shopify_log(
        status="Success",
        message="Triggered order/updated webhook for Shopify Order",
        response_data=f"#{url}\n {payload} \n {data}"
    )
    return "Request queued to Shopify"


def handle_shopify_return(payload, request_id=None):
    """
    Handles a Shopify return webhook.
    Updates the Delivery Note and its items in ERPNext based on Shopify order info.
    """
    import json

    try:
        shopify_order_id = str(payload.get("id") or payload.get("order_id"))
        returns = payload.get("returns", [])
        if not shopify_order_id or not returns:
            return


        delivery_notes = frappe.get_all(
            "Delivery Note",
            filters={"shopify_order_id": shopify_order_id},
            fields=["name"]
        )
        if not delivery_notes:
            create_shopify_log(status="Error",message=f"Delivery Note not found for Shopify Order ID {shopify_order_id}", exception=e, rollback=True)
            return

        dn_doc = frappe.get_doc("Delivery Note", delivery_notes[0]["name"])

        return_qty_map = {}
        for ret in returns:
            for rli in ret.get("return_line_items", []):
                line_item_id = str(rli.get("line_item_id"))
                qty = int(rli.get("quantity", 0))
                if line_item_id:
                    return_qty_map[line_item_id] = return_qty_map.get(line_item_id, 0) + qty

        updated = False
        item_id = ""
        for item in dn_doc.items:
            from ecommerce_integrations.shopify.fulfillment import get_shopify_item_and_variant_id
            _, variant_id = get_shopify_item_and_variant_id(item.item_code)
            shopify_line_item_id = None
            for li in payload.get("line_items", []):
                if str(li.get("variant_id")) == str(variant_id):
                    shopify_line_item_id = str(li.get("id"))
                    break
            if shopify_line_item_id and shopify_line_item_id in return_qty_map:
                item_id = item.item_code
                item.custom_expected_return_qty = return_qty_map[shopify_line_item_id]
                updated = True

        if updated:
            dn_doc.custom_return_requested = 1
            # dn_doc.flags.ignore_validate_update_after_submit = True
            dn_doc.save(ignore_permissions=True)
            frappe.db.commit()
            dn_doc.reload()
            create_shopify_log(status="Success",message="Delivery Note updated with Shopify return info.", response_data=f"#{dn_doc.name} \n {return_qty_map} \n {item_id}") 
        else:
            create_shopify_log(status="Error",message="No matching items found to update for this return.")
            return

    except Exception as e:
        create_shopify_log(status="Error", exception=e, rollback=True)
# Remove fulfillment_status custom field




# 2:49
# Remove 'Ready to Pick' filter




# 2:49
# Add create Pick List action