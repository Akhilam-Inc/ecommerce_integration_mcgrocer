import frappe
import json

from ecommerce_integrations.shopify.fulfillment import prepare_shopify_fulfillment


def sync_delivery_note_as_shopify_fulfilment(delivery_note_doc, method=None):
    """
    Syncs a delivery note as a Shopify fulfilment
    """

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