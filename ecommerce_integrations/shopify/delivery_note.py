import frappe
import json

from ecommerce_integrations.shopify.fulfillment import prepare_shopify_fulfillment


@frappe.whitelist()
def sync_delivery_note_as_shopify_fulfilment(delivery_note_doc, method=None):
    """
    Syncs a delivery note as a Shopify fulfilment
    """
    # check if delivery_not_doc is string in case json payload
    if delivery_note_doc and isinstance(delivery_note_doc, str):
        delivery_note_doc = frappe.get_doc("Delivery Note", json.loads(delivery_note_doc)["name"])

    if delivery_note_doc.docstatus == 2:
        # TODO: Cancelled Delivery Note
        return

    if delivery_note_doc.shopify_order_id and not delivery_note_doc.shopify_fulfillment_id and not delivery_note_doc.fulfilled_in_shopify:
        prepare_shopify_fulfillment(delivery_note_doc)

def cancel_delivery_note(delivery_note_doc, method=None):
    """
    Cancels a delivery note in Shopify
    """
    sales_order = frappe.get_doc("Sales Order", delivery_note_doc.sales_order)
    sales.fulfillment_status = "Ready to Pack"
    sales_order.save()