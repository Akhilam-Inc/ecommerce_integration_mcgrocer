import frappe
from shopify.resources import Product
from pyactiveresource.connection import ResourceNotFound
from ecommerce_integrations.shopify.constants import MODULE_NAME, SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.shopify.connection import temp_shopify_session


@temp_shopify_session
def find_or_create_product(product_id, item_doc=None):
    """
    Find a Shopify product by ID. If it doesn't exist, recreate it and update the Ecommerce Item record.
    
    Args:
        product_id (str): The Shopify product ID to find
        item_doc (Document, optional): The ERPNext Item document for recreation
    
    Returns:
        Product or None: The found or recreated Shopify product
    """
    try:
        # Try to find the product
        product = Product.find(product_id)
        return product
    
    except ResourceNotFound as e:
        # Handle the specific ResourceNotFound exception
        create_shopify_log(
            message=f"Product {product_id} not found on Shopify. Attempting to recreate...",
            status="Warning",
            exception=e
        )
        
        # If no item_doc provided, try to get it from Ecommerce Item
        if not item_doc:
            item_doc = _get_item_from_ecommerce_item(product_id)
        
        if item_doc:
            # Recreate the product
            new_product = _recreate_product(item_doc, product_id)
            return new_product
        else:
            create_shopify_log(
                message=f"Cannot recreate product {product_id} - no ERPNext Item found",
                status="Error"
            )
            return None
    
    except Exception as e:
        # Re-raise other exceptions
        create_shopify_log(
            message=f"Unexpected error finding product {product_id}: {str(e)}",
            status="Error",
            exception=e
        )
        raise e


def _get_item_from_ecommerce_item(product_id):
    """Get the ERPNext Item document from Ecommerce Item record."""
    try:
        ecom_item = frappe.get_doc("Ecommerce Item", {
            "integration_item_code": product_id,
            "integration": MODULE_NAME
        })
        
        # Get the template item (parent) if this is a variant
        if ecom_item.variant_of:
            return frappe.get_doc("Item", ecom_item.variant_of)
        else:
            return frappe.get_doc("Item", ecom_item.erpnext_item_code)
            
    except frappe.DoesNotExistError:
        return None


@temp_shopify_session  
def _recreate_product(item_doc, old_product_id):
    """Recreate a Shopify product and update all related Ecommerce Item records."""
    from ecommerce_integrations.shopify.product import (
        map_erpnext_item_to_shopify, 
        update_default_variant_properties,
        get_shopify_weight_uom
    )
    from ecommerce_integrations.shopify.constants import ITEM_SELLING_RATE_FIELD
    
    try:
        setting = frappe.get_doc(SETTING_DOCTYPE)
        
        # Create new product
        product = Product()
        product.published = False
        product.status = "active" if setting.sync_new_item_as_active else "draft"
        
        # Map ERPNext item to Shopify
        map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=item_doc)
        
        create_shopify_log(
            message=f"Recreating product for ERPNext item: {item_doc.name}",
            status="Info",
            request_data=product.to_dict(),
        )
        
        is_successful = product.save()
        
        if is_successful:
            create_shopify_log(
                message=f"Successfully recreated product. New ID: {product.id}, Old ID: {old_product_id}",
                status="Success",
                response_data=product.to_dict(),
            )
            
            # Update default variant properties
            weight_to_sync = max(item_doc.weight_per_unit or 0, item_doc.volumetric_weight or 0)
            update_default_variant_properties(
                product,
                sku=item_doc.item_code,
                is_stock_item=item_doc.is_stock_item,
                price=item_doc.get(ITEM_SELLING_RATE_FIELD),
                weight=weight_to_sync,
                weight_unit=get_shopify_weight_uom(erpnext_weight_uom=item_doc.weight_uom) if item_doc.weight_uom else None
            )
            
            # Update all related Ecommerce Item records
            _update_ecommerce_items(old_product_id, str(product.id), product)
            
            return product
        else:
            create_shopify_log(
                message=f"Failed to recreate product for item: {item_doc.name}",
                status="Error",
                exception=product.errors.full_messages(),
            )
            return None
            
    except Exception as e:
        create_shopify_log(
            message=f"Error recreating product for item: {item_doc.name}",
            status="Error",
            exception=e
        )
        return None


def _update_ecommerce_items(old_product_id, new_product_id, shopify_product):
    """Update all Ecommerce Item records with new product and variant IDs."""
    try:
        # Get all Ecommerce Items with the old product ID
        ecom_items = frappe.get_all(
            "Ecommerce Item",
            filters={
                "integration_item_code": old_product_id,
                "integration": MODULE_NAME
            },
            fields=["name", "erpnext_item_code", "variant_of", "has_variants"]
        )
        
        for ecom_item_data in ecom_items:
            ecom_item = frappe.get_doc("Ecommerce Item", ecom_item_data.name)
            
            # Update product ID
            ecom_item.integration_item_code = new_product_id
            
            # Update variant ID and SKU if this is not a template item
            if not ecom_item_data.has_variants:
                if shopify_product.variants:
                    # For template items (non-variants), use the first variant
                    if not ecom_item_data.variant_of:
                        ecom_item.variant_id = str(shopify_product.variants[0].id)
                        ecom_item.sku = str(shopify_product.variants[0].sku)
                    else:
                        # For variant items, find matching variant by SKU
                        item_doc = frappe.get_doc("Item", ecom_item_data.erpnext_item_code)
                        matching_variant = None
                        
                        for variant in shopify_product.variants:
                            if variant.sku == item_doc.item_code:
                                matching_variant = variant
                                break
                        
                        if matching_variant:
                            ecom_item.variant_id = str(matching_variant.id)
                            ecom_item.sku = str(matching_variant.sku)
                        else:
                            # If no matching variant found, use the first one
                            ecom_item.variant_id = str(shopify_product.variants[0].id)
                            ecom_item.sku = str(shopify_product.variants[0].sku)
            
            ecom_item.save(ignore_permissions=True)
            
            create_shopify_log(
                message=f"Updated Ecommerce Item {ecom_item.name}: {old_product_id} -> {new_product_id}",
                status="Success"
            )
        
        frappe.db.commit()
        
    except Exception as e:
        create_shopify_log(
            message=f"Error updating Ecommerce Items for product {old_product_id}",
            status="Error",
            exception=e
        )
        frappe.db.rollback()