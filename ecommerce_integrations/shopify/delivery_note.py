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
       
    if delivery_note_doc.shopify_order_id:
      prepare_shopify_fulfillment( delivery_note_doc)