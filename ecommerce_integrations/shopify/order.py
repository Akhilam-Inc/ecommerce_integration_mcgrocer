import json
from typing import Literal, Optional

import frappe
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, getdate, nowdate, add_days
from shopify.collection import PaginatedIterator
from shopify.resources import Order

from erpnext.controllers.accounts_controller import update_child_qty_rate

from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
    CUSTOMER_ID_FIELD,
    EVENT_MAPPER,
    ORDER_ID_FIELD,
    ORDER_ITEM_DISCOUNT_FIELD,
    ORDER_NUMBER_FIELD,
    ORDER_STATUS_FIELD,
    SETTING_DOCTYPE,
)
from ecommerce_integrations.shopify.customer import ShopifyCustomer
from ecommerce_integrations.shopify.product import create_items_if_not_exist, get_item_code
from ecommerce_integrations.shopify.shopify_order_manager import ShopifyOrderManager
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.utils.price_list import get_dummy_price_list
from ecommerce_integrations.utils.taxation import get_dummy_tax_category

DEFAULT_TAX_FIELDS = {
    "sales_tax": "default_sales_tax_account",
    "shipping": "default_shipping_charges_account",
}

shopify_setting = frappe.get_doc(SETTING_DOCTYPE)
def sync_sales_order(payload, request_id=None):
    order = payload
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id

    if frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: cstr(order["id"])}):
        create_shopify_log(status="Invalid", message="Sales order already exists, not synced")
        return
    try:
        shopify_customer = order.get("customer") if order.get("customer") is not None else {}
        shopify_customer["billing_address"] = order.get("billing_address", "")
        shopify_customer["shipping_address"] = order.get("shipping_address", "")
        customer_id = shopify_customer.get("id")
        if customer_id:
            customer = ShopifyCustomer(customer_id=customer_id)
            if not customer.is_synced():
                customer.sync_customer(customer=shopify_customer)
            else:
                customer.update_existing_addresses(shopify_customer)

        create_items_if_not_exist(order)

        setting = frappe.get_doc(SETTING_DOCTYPE)
        new_so_doc = create_order(order, setting)
        
        if shopify_setting.send_success_email:
            # Get customer email from the new Sales Order's first address
            customer_email = frappe.db.get_value("Address", {"name": new_so_doc.customer_address}, "email_id")
            if customer_email:
                recipients = [customer_email]
                frappe.sendmail(
                    recipients=recipients,
                    subject=f"Your Order is being processed: [{order['id']}]",
                    message=f"Shopify Order ({order['id']}) is being processed.",
                )
    except Exception as e:
        new_log = create_shopify_log(status="Error", exception=e, rollback=True)
        
        # Get all sales managers' emails
        sales_managers = frappe.get_all("User", filters={"role_profile_name": "Sales Manager"}, fields=["email"])
        recipients = [manager.email for manager in sales_managers]
        
        if shopify_setting.send_fail_email:
            if len(recipients) > 0:
                frappe.sendmail(
                recipients=recipients,
                subject=f"Shopify Order Sync failed: Order {order['id'] or ''}",
                message=(
                    "Shopify Order could not be synchronized due to an error.\n"
                    "Check ecommerce intergration log for more details "
                    f"<a href='{frappe.utils.get_url()}/app/ecommerce-integration-log/{new_log.name}'>here</a>"
                ),
            )
    else:
        create_shopify_log(status="Success")

def create_order(order, setting, company=None):
    # local import to avoid circular dependencies
    from ecommerce_integrations.shopify.fulfillment import create_delivery_note
    from ecommerce_integrations.shopify.invoice import create_sales_invoice

    so = create_sales_order(order, setting, company)
    if so:
        if order.get("financial_status") == "paid":
            create_sales_invoice(order, setting, so)

        if order.get("fulfillments"):
            create_delivery_note(order, setting, so)
    return so
    

def create_sales_order(shopify_order, setting, company=None):
    customer = setting.default_customer
    if shopify_order.get("customer", {}):
        if customer_id := shopify_order.get("customer", {}).get("id"):
            customer = frappe.db.get_value("Customer", {CUSTOMER_ID_FIELD: customer_id}, "name")

    so = frappe.db.get_value("Sales Order", {ORDER_ID_FIELD: shopify_order.get("id")}, "name")

    if not so:
        delivery_date = getdate(shopify_order.get("created_at")) or nowdate()

        shipping_title = get_shipping_title(shopify_order)
        print(f"{str(nowdate())}: Shipping Title: {shipping_title}")
        shipping_country = get_shipping_country(shopify_order)
        if shipping_title and shipping_country:        
            min_delivery_date = get_shipping_minimum_delivery_days(shipping_title, shipping_country)
            print(f"{str(nowdate())}: Minimum Delivery Date: {min_delivery_date}")
            if min_delivery_date:
                delivery_date = add_days(delivery_date, int(min_delivery_date))
    
        items = get_order_items(
            shopify_order.get("line_items"),
            setting,
            delivery_date, 
            taxes_inclusive=shopify_order.get("taxes_included"),
        )

        if not items:
            message = (
                "Following items exists in the shopify order but relevant records were"
                " not found in the shopify Product master"
            )
            product_not_exists = []  # TODO: fix missing items
            message += "\n" + ", ".join(product_not_exists)

            create_shopify_log(status="Error", exception=message, rollback=True)
            
            return ""

        taxes = get_order_taxes(shopify_order, setting, items)
        
        so = frappe.get_doc(
            {
                "doctype": "Sales Order",
                "naming_series": setting.sales_order_series or "SO-Shopify-",
                ORDER_ID_FIELD: str(shopify_order.get("id")),
                ORDER_NUMBER_FIELD: shopify_order.get("name"),
                "customer": customer,
                "transaction_date": getdate(shopify_order.get("created_at")) or nowdate(),
                "delivery_date": delivery_date,
                "company": setting.company,
                "selling_price_list": get_dummy_price_list(),
                "ignore_pricing_rule": 1,
                "items": items,
                "taxes": taxes,
                "tax_category": get_dummy_tax_category(),
            }
        )

        if company:
            so.update({"company": company, "status": "Draft"})
        so.flags.ignore_mandatory = True
        so.flags.shopiy_order_json = json.dumps(shopify_order)
        so.save(ignore_permissions=True)
        so.submit()

        if shopify_order.get("note"):
            so.add_comment(text=f"Order Note: {shopify_order.get('note')}")

    else:
        so = frappe.get_doc("Sales Order", so)

    return so

def get_shipping_minimum_delivery_days(shipping_line_title, shipping_country):     
    shipping_rule = frappe.db.sql("""
        SELECT sr.name, sr.custom_minimum_delivery_days
        FROM `tabShipping Rule` sr
        JOIN `tabShipping Rule Country` src ON sr.name = src.parent
        WHERE sr.custom_display_name LIKE %s AND src.country = %s
        LIMIT 1
    """, (f"%{shipping_line_title}%", shipping_country), as_dict=True)

    if shipping_rule:
        return shipping_rule[0].custom_minimum_delivery_days

    return None

def get_shipping_title(shopify_order):
    shopify_shipping_lines  = shopify_order.get("shipping_lines")
    if  shopify_shipping_lines and isinstance(shopify_shipping_lines, list):
        return shopify_shipping_lines[0].get("title")
    return None

def get_shipping_country(shopify_order):
    shopify_address  = shopify_order.get("shipping_address")
    if  shopify_address and isinstance(shopify_address, dict):
        return shopify_address.get("country")
    return None

def get_order_items(order_items, setting, delivery_date, taxes_inclusive):
    items = []
    all_product_exists = True
    product_not_exists = []

    for shopify_item in order_items:
        if not shopify_item.get("product_exists"):
            all_product_exists = False
            product_not_exists.append(
                {"title": shopify_item.get("title"), ORDER_ID_FIELD: shopify_item.get("id")}
            )
            continue

        if all_product_exists:
            item_code = get_item_code(shopify_item)
            items.append(
                {
                    "item_code": item_code,
                    "item_name": shopify_item.get("name"),
                    "rate": _get_item_price(shopify_item, taxes_inclusive),
                    "delivery_date": delivery_date,
                    "qty": shopify_item.get("quantity"),
                    "stock_uom": shopify_item.get("uom") or "Nos",
                    "warehouse": setting.warehouse,
                    ORDER_ITEM_DISCOUNT_FIELD: (
                        _get_total_discount(shopify_item) / cint(shopify_item.get("quantity"))
                    ),
                }
            )
        else:
            items = []

    return items


def _get_item_price(line_item, taxes_inclusive: bool) -> float:

    price = flt(line_item.get("price"))
    qty = cint(line_item.get("quantity"))

    # remove line item level discounts
    total_discount = _get_total_discount(line_item)

    if not taxes_inclusive:
        return price - (total_discount / qty)

    total_taxes = 0.0
    for tax in line_item.get("tax_lines"):
        total_taxes += flt(tax.get("price"))

    return price - (total_taxes + total_discount) / qty


def _get_total_discount(line_item) -> float:
    discount_allocations = line_item.get("discount_allocations") or []
    return sum(flt(discount.get("amount")) for discount in discount_allocations)


def get_order_taxes(shopify_order, setting, items):
    taxes = []
    line_items = shopify_order.get("line_items")

    for line_item in line_items:
        item_code = get_item_code(line_item)
        for tax in line_item.get("tax_lines"):
            taxes.append(
                {
                    "charge_type": "Actual",
                    "account_head": get_tax_account_head(tax, charge_type="sales_tax"),
                    "description": (
                        get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
                    ),
                    "tax_amount": tax.get("price"),
                    "included_in_print_rate": 0,
                    "cost_center": setting.cost_center,
                    "item_wise_tax_detail": {item_code: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]},
                    "dont_recompute_tax": 1,
                }
            )

    update_taxes_with_shipping_lines(
        taxes,
        shopify_order.get("shipping_lines"),
        setting,
        items,
        taxes_inclusive=shopify_order.get("taxes_included"),
    )

    if cint(setting.consolidate_taxes):
        taxes = consolidate_order_taxes(taxes)

    for row in taxes:
        tax_detail = row.get("item_wise_tax_detail")
        if isinstance(tax_detail, dict):
            row["item_wise_tax_detail"] = json.dumps(tax_detail)

    return taxes


def consolidate_order_taxes(taxes):
    tax_account_wise_data = {}
    for tax in taxes:
        account_head = tax["account_head"]
        tax_account_wise_data.setdefault(
            account_head,
            {
                "charge_type": "Actual",
                "account_head": account_head,
                "description": tax.get("description"),
                "cost_center": tax.get("cost_center"),
                "included_in_print_rate": 0,
                "dont_recompute_tax": 1,
                "tax_amount": 0,
                "item_wise_tax_detail": {},
            },
        )
        tax_account_wise_data[account_head]["tax_amount"] += flt(tax.get("tax_amount"))
        if tax.get("item_wise_tax_detail"):
            tax_account_wise_data[account_head]["item_wise_tax_detail"].update(tax["item_wise_tax_detail"])

    return tax_account_wise_data.values()


def get_tax_account_head(tax, charge_type: Optional[Literal["shipping", "sales_tax"]] = None):
    tax_title = str(tax.get("title"))

    tax_account = frappe.db.get_value(
        "Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_account",
    )

    if not tax_account and charge_type:
        tax_account = frappe.db.get_single_value(SETTING_DOCTYPE, DEFAULT_TAX_FIELDS[charge_type])

    if not tax_account:
        frappe.throw(_("Tax Account not specified for Shopify Tax {0}").format(tax.get("title")))

    return tax_account


def get_tax_account_description(tax):
    tax_title = tax.get("title")

    tax_description = frappe.db.get_value(
        "Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_description",
    )

    return tax_description


def update_taxes_with_shipping_lines(taxes, shipping_lines, setting, items, taxes_inclusive=False):
    """Shipping lines represents the shipping details,
    each such shipping detail consists of a list of tax_lines"""
    shipping_as_item = cint(setting.add_shipping_as_item) and setting.shipping_item
    for shipping_charge in shipping_lines:
        if shipping_charge.get("price"):
            shipping_discounts = shipping_charge.get("discount_allocations") or []
            total_discount = sum(flt(discount.get("amount")) for discount in shipping_discounts)

            shipping_taxes = shipping_charge.get("tax_lines") or []
            total_tax = sum(flt(discount.get("price")) for discount in shipping_taxes)

            shipping_charge_amount = flt(shipping_charge["price"]) - flt(total_discount)
            if bool(taxes_inclusive):
                shipping_charge_amount -= total_tax

            if shipping_as_item:
                items.append(
                    {
                        "item_code": setting.shipping_item,
                        "rate": shipping_charge_amount,
                        "delivery_date": items[-1]["delivery_date"] if items else nowdate(),
                        "qty": 1,
                        "stock_uom": "Nos",
                        "warehouse": setting.warehouse,
                    }
                )
            else:
                taxes.append(
                    {
                        "charge_type": "Actual",
                        "account_head": get_tax_account_head(shipping_charge, charge_type="shipping"),
                        "description": get_tax_account_description(shipping_charge) or shipping_charge["title"],
                        "tax_amount": shipping_charge_amount,
                        "cost_center": setting.cost_center,
                    }
                )

        for tax in shipping_charge.get("tax_lines"):
            taxes.append(
                {
                    "charge_type": "Actual",
                    "account_head": get_tax_account_head(tax, charge_type="sales_tax"),
                    "description": (
                        get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
                    ),
                    "tax_amount": tax["price"],
                    "cost_center": setting.cost_center,
                    "item_wise_tax_detail": {
                        setting.shipping_item: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]
                    }
                    if shipping_as_item
                    else {},
                    "dont_recompute_tax": 1,
                }
            )


def get_sales_order(order_id):
    """Get ERPNext sales order using shopify order id."""
    sales_order = frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: order_id})
    if sales_order:
        return frappe.get_doc("Sales Order", sales_order)


def cancel_order(payload, request_id=None):
    """Called by order/cancelled event.

    When shopify order is cancelled there could be many different someone handles it.

    Updates document with custom field showing order status.

    IF sales invoice / delivery notes are not generated against an order, then cancel it.
    """
    frappe.set_user("Administrator")
    frappe.flags.request_id = request_id

    order = payload

    try:
        order_id = order["id"]
        order_status = order["financial_status"]

        sales_order = get_sales_order(order_id)

        if not sales_order:
            create_shopify_log(status="Invalid", message="Sales Order does not exist")
            return

        sales_invoice = frappe.db.get_value("Sales Invoice", filters={ORDER_ID_FIELD: order_id})
        delivery_notes = frappe.db.get_list("Delivery Note", filters={ORDER_ID_FIELD: order_id})

        if sales_invoice:
            frappe.db.set_value("Sales Invoice", sales_invoice, ORDER_STATUS_FIELD, order_status)

        for dn in delivery_notes:
            frappe.db.set_value("Delivery Note", dn.name, ORDER_STATUS_FIELD, order_status)

        if not sales_invoice and not delivery_notes and sales_order.docstatus == 1:
            sales_order.cancel()
        else:
            frappe.db.set_value("Sales Order", sales_order.name, ORDER_STATUS_FIELD, order_status)

    except Exception as e:
        create_shopify_log(status="Error", exception=e)
    else:
        create_shopify_log(status="Success")


@temp_shopify_session
def sync_old_orders():
    shopify_setting = frappe.get_cached_doc(SETTING_DOCTYPE)
    if not cint(shopify_setting.sync_old_orders):
        return

    orders = _fetch_old_orders(shopify_setting.old_orders_from, shopify_setting.old_orders_to)

    for order in orders:
        log = create_shopify_log(
            method=EVENT_MAPPER["orders/create"], request_data=json.dumps(order), make_new=True
        )
        sync_sales_order(order, request_id=log.name)

    shopify_setting = frappe.get_doc(SETTING_DOCTYPE)
    shopify_setting.sync_old_orders = 0
    shopify_setting.save()


def _fetch_old_orders(from_time, to_time):
    """Fetch all shopify orders in specified range and return an iterator on fetched orders."""

    from_time = get_datetime(from_time).astimezone().isoformat()
    to_time = get_datetime(to_time).astimezone().isoformat()
    orders_iterator = PaginatedIterator(
        Order.find(created_at_min=from_time, created_at_max=to_time, limit=250)
    )

    for orders in orders_iterator:
        for order in orders:
            # Using generator instead of fetching all at once is better for
            # avoiding rate limits and reducing resource usage.
            yield order.to_dict()

def sort_items_for_sync(active_erpnext_items, active_shopify_items, item_mapping, erpnext_existing_items, erpnext_order_name, delivery_date, shopify_settings):
    trans_items = []

    # Add or update items from active Shopify items
    for idx, (product_id, shopify_item) in enumerate(active_shopify_items.items(), start=1):
        erpnext_item_code = item_mapping.get(product_id)
        if not erpnext_item_code:
            continue

        if erpnext_item_code in erpnext_existing_items:
            trans_items.append({
                "docname": erpnext_existing_items[erpnext_item_code].name,
                "name": erpnext_existing_items[erpnext_item_code].name,
                "item_code": erpnext_item_code,
                "delivery_date": str(delivery_date),  # Convert to string
                "conversion_factor": 1,
                "qty": shopify_item['current_quantity'],
                "rate": shopify_item['price'],
                "uom": erpnext_existing_items[erpnext_item_code].uom,
                "idx": idx
            })
        else:
            item_details = frappe.db.get_value(
                'Item', erpnext_item_code, ['item_name', 'stock_uom'], as_dict=1
            )
            trans_items.append({
                "doctype": "Sales Order Item",
                "parent": erpnext_order_name,
                "parenttype": "Sales Order",
                "parentfield": "items",
                "item_code": erpnext_item_code,
                "item_name": item_details.item_name,
                "qty": shopify_item['current_quantity'],
                "warehouse": shopify_settings.warehouse,
                "rate": shopify_item['price'],
                "uom": item_details.stock_uom,
                "stock_uom": item_details.stock_uom,
                "conversion_factor": 1,
                "delivery_date": str(delivery_date),  # Convert to string
                "idx": idx,
                "__islocal": True
            })

    # Add existing items that are not in the active Shopify items to be removed
    for item_code in active_erpnext_items:
        shopify_product_id = None
        for pid, erpid in item_mapping.items():
            if erpid == item_code:
                shopify_product_id = pid
                break
        if not shopify_product_id or shopify_product_id not in active_shopify_items:
            # Do not include items with qty 0 in the trans_items payload
            continue

    return trans_items


def update_sales_order_items(erpnext_order_name, trans_items):
    trans_items_json = json.dumps(trans_items)
    update_child_qty_rate("Sales Order", trans_items_json, erpnext_order_name)


def sync_sales_order_items(payload, request_id=None):
    shopify_settings = frappe.get_doc(SETTING_DOCTYPE)

    if not shopify_settings.sync_edited_orders:
        create_shopify_log(status="Invalid", message="Sync Sales Order Items is disabled")
        return

    order = payload
    try:
        frappe.set_user('Administrator')
        shopify_order_id = order["order_edit"]["order_id"]
        shopify_order = get_shopify_order(shopify_settings, shopify_order_id)
        # Try to get the ERPNext order
        erpnext_order_list = frappe.db.get_list(
            "Sales Order", filters={"shopify_order_id": shopify_order_id}, fields=["name", "delivery_date"]
        )
        if not erpnext_order_list:
            # Order does not exist, create it using your existing function
            create_order(shopify_order, shopify_settings)
            create_shopify_log(message=f"Sales Order created for Shopify Order ID '{shopify_order_id}'", status="Success")
            return True  # No need to sync items, just created
        erpnext_order = erpnext_order_list[0]

        erpnext_existing_items, active_erpnext_items = get_erpnext_existing_items(erpnext_order.name)
        active_shopify_items = get_active_shopify_items(shopify_order)
        item_mapping = get_item_mapping(active_shopify_items)

        try:
            trans_items = sort_items_for_sync(
                active_erpnext_items, active_shopify_items, item_mapping, erpnext_existing_items, erpnext_order.name, erpnext_order.delivery_date, shopify_settings
            )

            update_sales_order_items(erpnext_order.name, trans_items)

            frappe.db.commit()
            create_shopify_log(message=f"Order updated '{shopify_order_id}'", status="Success")
            return True

        finally:
            from mcgrocer_customization.mcgrocer_customization.controllers.sales_order import update_sales_order_pick_status
            update_sales_order_pick_status(erpnext_order)
            frappe.set_user('Guest')

    except (KeyError) as e:
        create_shopify_log(message=f"Shopify order not found - '{e}'", exception=e, status="Error", request_data=json.dumps(payload))
    except (Exception, frappe.exceptions.ValidationError, frappe.exceptions.DoesNotExistError, ValueError) as e:
        create_shopify_log(message=f"Error syncing order items from Shopify - '{e}'", status="Error", exception=e, rollback=True)


def get_shopify_order(shopify_settings, shopify_order_id):
    shopify_manager = ShopifyOrderManager(shopify_settings.shopify_url, shopify_settings.get_password('password'))
    return shopify_manager.get_order(shopify_order_id)['order']


def get_erpnext_order(shopify_order_id):
    return frappe.db.get_list("Sales Order", filters={"shopify_order_id": shopify_order_id}, fields=["name", "delivery_date"])[0]


def get_erpnext_existing_items(erpnext_order_name):
    erpnext_sales_order_items = frappe.get_all(
        "Sales Order Item",
        filters={"parent": erpnext_order_name},
        fields=["name", "item_code", "item_name", "qty", "uom", "stock_uom", "idx"]
    )
    erpnext_existing_items = {item.item_code: item for item in erpnext_sales_order_items}
    active_erpnext_items = set(erpnext_existing_items.keys())
    return erpnext_existing_items, active_erpnext_items


def get_active_shopify_items(shopify_order):
    return {
        str(item['product_id']): item
        for item in shopify_order['line_items']
        if item['current_quantity'] > 0
    }


def get_item_mapping(active_shopify_items):
    ecommerce_items = frappe.get_all(
        'Ecommerce Item',
        filters={
            'integration': 'shopify',
            'integration_item_code': ['in', list(active_shopify_items.keys())]
        },
        fields=['integration_item_code', 'erpnext_item_code']
    )
    return {item['integration_item_code']: item['erpnext_item_code'] for item in ecommerce_items}


def update_shopify_fulfillment(delivery_note):
    """Update Shopify order fulfillment based on ERPNext delivery note."""
    shopify_settings = frappe.get_doc(SETTING_DOCTYPE)
    shopify_order_id = frappe.db.get_value("Sales Order", delivery_note.sales_order, ORDER_ID_FIELD)

    if not shopify_order_id:
        frappe.throw(_("Shopify Order ID not found for Sales Order {0}").format(delivery_note.sales_order))

    shopify_manager = ShopifyOrderManager(shopify_settings.shopify_url, shopify_settings.get_password('password'))
    fulfillment_data = {
        "fulfillment": {
            "location_id": shopify_settings.shopify_location_id,
            "tracking_number": delivery_note.tracking_number,
            "tracking_urls": [delivery_note.tracking_url],
            "line_items": [
                {
                    "id": frappe.db.get_value("Sales Order Item", {"parent": delivery_note.sales_order, "item_code": item.item_code}, "shopify_line_item_id"),
                    "quantity": item.qty
                }
                for item in delivery_note.items
            ]
        }
    }

    try:
        shopify_manager.create_fulfillment(shopify_order_id, fulfillment_data)
        create_shopify_log(status="Success", message=f"Fulfillment updated for Shopify Order {shopify_order_id}")
    except Exception as e:
        create_shopify_log(status="Error", exception=e, message=f"Failed to update fulfillment for Shopify Order {shopify_order_id}")