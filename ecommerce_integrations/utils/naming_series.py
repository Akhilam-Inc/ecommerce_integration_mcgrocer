import frappe


@frappe.whitelist()
def get_series():
	return {
		"sales_order_series": frappe.get_meta("Sales Order").get_options("naming_series"),
		"sales_invoice_series": frappe.get_meta("Sales Invoice").get_options("naming_series"),
		"delivery_note_series": frappe.get_meta("Delivery Note").get_options("naming_series"),
	}

def sales_order_custom_naming(doc, method):
    # If shopify_order_number is set, use it
    if getattr(doc, "shopify_order_number", None):
        doc.name = f"MC-{doc.shopify_order_number}"
    # Otherwise, fallback to default naming series
    else:
        doc.name = None  # Let ERPNext use the default naming series
