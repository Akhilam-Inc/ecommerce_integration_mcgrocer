from typing import Optional

import frappe
from frappe import _, msgprint
from frappe.utils import cint, cstr
from frappe.utils.nestedset import get_root_of
from shopify.resources import Product, Variant, Image
import requests
import frappe.utils.file_manager

from ecommerce_integrations.ecommerce_integrations.doctype.ecommerce_item import ecommerce_item
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
  ITEM_SELLING_RATE_FIELD,
  MODULE_NAME,
  SETTING_DOCTYPE,
  SHOPIFY_VARIANTS_ATTR_LIST,
  SUPPLIER_ID_FIELD,
  WEIGHT_TO_ERPNEXT_UOM_MAP,
)
from ecommerce_integrations.shopify.custom_inventory import update_inventory
from ecommerce_integrations.shopify.meta_fields import add_ai_summary, add_ai_title
from ecommerce_integrations.shopify.collection import add_product_to_collections_from_breadcrumb
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.shopify.shopify_product_utils import find_or_create_product


class ShopifyProduct:
  def __init__(
    self,
    product_id: str,
    variant_id: str | None = None,
    sku: str | None = None,
    has_variants: int | None = 0,
  ):
    self.product_id = str(product_id)
    self.variant_id = str(variant_id) if variant_id else None
    self.sku = str(sku) if sku else None
    self.has_variants = has_variants
    self.setting = frappe.get_doc(SETTING_DOCTYPE)

    if not self.setting.is_enabled():
      frappe.throw(_("Can not create Shopify product when integration is disabled."))

  def is_synced(self) -> bool:
    return ecommerce_item.is_synced(
      MODULE_NAME,
      integration_item_code=self.product_id,
      variant_id=self.variant_id,
      sku=self.sku,
    )

  def get_erpnext_item(self):
    return ecommerce_item.get_erpnext_item(
      MODULE_NAME,
      integration_item_code=self.product_id,
      variant_id=self.variant_id,
      sku=self.sku,
      has_variants=self.has_variants,
    )

  @temp_shopify_session
  def sync_product(self):
    if not self.is_synced():
      shopify_product = Product.find(self.product_id)
      product_dict = shopify_product.to_dict()
      self._make_item(product_dict)

  def _make_item(self, product_dict):
    _add_weight_details(product_dict)

    warehouse = self.setting.warehouse

    if _has_variants(product_dict):
      self.has_variants = 1
      attributes = self._create_attribute(product_dict)
      self._create_item(product_dict, warehouse, 1, attributes)
      self._create_item_variants(product_dict, warehouse, attributes)

    else:
      product_dict["variant_id"] = product_dict["variants"][0]["id"]
      self._create_item(product_dict, warehouse)

  def _create_attribute(self, product_dict):
    attribute = []
    for attr in product_dict.get("options"):
      if not frappe.db.get_value("Item Attribute", attr.get("name"), "name"):
        frappe.get_doc(
          {
            "doctype": "Item Attribute",
            "attribute_name": attr.get("name"),
            "item_attribute_values": [
              {"attribute_value": attr_value, "abbr": attr_value}
              for attr_value in attr.get("values")
            ],
          }
        ).insert()
        attribute.append({"attribute": attr.get("name")})

      else:
        # check for attribute values
        item_attr = frappe.get_doc("Item Attribute", attr.get("name"))
        if not item_attr.numeric_values:
          self._set_new_attribute_values(item_attr, attr.get("values"))
          item_attr.save()
          attribute.append({"attribute": attr.get("name")})

        else:
          attribute.append(
            {
              "attribute": attr.get("name"),
              "from_range": item_attr.get("from_range"),
              "to_range": item_attr.get("to_range"),
              "increment": item_attr.get("increment"),
              "numeric_values": item_attr.get("numeric_values"),
            }
          )

    return attribute

  def _set_new_attribute_values(self, item_attr, values):
    for attr_value in values:
      if not any(
        (d.abbr.lower() == attr_value.lower() or d.attribute_value.lower() == attr_value.lower())
        for d in item_attr.item_attribute_values
      ):
        item_attr.append("item_attribute_values", {"attribute_value": attr_value, "abbr": attr_value})

  def _create_item(self, product_dict, warehouse, has_variant=0, attributes=None, variant_of=None):
    item_dict = {
      "variant_of": variant_of,
      "is_stock_item": 1,
      "item_code": cstr(product_dict.get("item_code")) or cstr(product_dict.get("id")),
      "item_name": product_dict.get("title", "").strip(),
      "description": product_dict.get("body_html") or product_dict.get("title"),
      "item_group": self._get_item_group(product_dict.get("product_type")),
      "has_variants": has_variant,
      "attributes": attributes or [],
      "stock_uom": product_dict.get("uom") or _("Nos"),
      "sku": product_dict.get("sku") or _get_sku(product_dict),
      "default_warehouse": warehouse,
      "image": _get_item_image(product_dict),
      "weight_uom": WEIGHT_TO_ERPNEXT_UOM_MAP[product_dict.get("weight_unit")],
      "weight_per_unit": product_dict.get("weight"),
      "weight": product_dict.get("weight"),
      "default_supplier": self._get_supplier(product_dict),
    }

    integration_item_code = product_dict["id"]  # shopify product_id
    variant_id = product_dict.get("variant_id", "")  # shopify variant_id if has variants
    sku = item_dict["sku"]

    if not _match_sku_and_link_item(
      item_dict, integration_item_code, variant_id, variant_of=variant_of, has_variant=has_variant
    ):
      ecommerce_item_doc = ecommerce_item.create_ecommerce_item(
        MODULE_NAME,
        integration_item_code,
        item_dict,
        variant_id=variant_id,
        sku=sku,
        variant_of=variant_of,
        has_variants=has_variant,
      )

      item_code = ecommerce_item_doc.erpnext_item_code
      if item_code and product_dict.get("vendor"):
        supplier_docname = self._get_supplier(product_dict)
        if supplier_docname:
          self._create_item_supplier(item_code, supplier_docname, product_dict["variants"][0].get("price") or 0)

  def _create_item_variants(self, product_dict, warehouse, attributes):
    template_item = ecommerce_item.get_erpnext_item(
      MODULE_NAME, integration_item_code=product_dict.get("id"), has_variants=1
    )

    if template_item:
      for variant in product_dict.get("variants"):
        shopify_item_variant = {
          "id": product_dict.get("id"),
          "variant_id": variant.get("id"),
          "item_code": variant.get("id"),
          "title": product_dict.get("title", "").strip() + "-" + variant.get("title"),
          "product_type": product_dict.get("product_type"),
          "sku": variant.get("sku"),
          "uom": template_item.stock_uom or _("Nos"),
          "item_price": variant.get("price"),
          "weight_unit": variant.get("weight_unit"),
          "weight": variant.get("weight"),
        }

        for i, variant_attr in enumerate(SHOPIFY_VARIANTS_ATTR_LIST):
          if variant.get(variant_attr):
            attributes[i].update(
              {
                "attribute_value": self._get_attribute_value(
                  variant.get(variant_attr), attributes[i]
                )
              }
            )
        self._create_item(shopify_item_variant, warehouse, 0, attributes, template_item.name)

  def _get_attribute_value(self, variant_attr_val, attribute):
    attribute_value = frappe.db.sql(
      """select attribute_value from `tabItem Attribute Value`
      where parent = %s and (abbr = %s or attribute_value = %s)""",
      (attribute["attribute"], variant_attr_val, variant_attr_val),
      as_list=1,
    )
    return attribute_value[0][0] if len(attribute_value) > 0 else cint(variant_attr_val)

  def _get_item_group(self, product_type=None):
    parent_item_group = get_root_of("Item Group")

    if not product_type:
      return parent_item_group

    if frappe.db.get_value("Item Group", product_type, "name"):
      return product_type
    item_group = frappe.get_doc(
      {
        "doctype": "Item Group",
        "item_group_name": product_type,
        "parent_item_group": parent_item_group,
        "is_group": "No",
      }
    ).insert()
    return item_group.name

  def _get_supplier(self, product_dict):
    if product_dict.get("vendor"):
      supplier = frappe.db.sql(
        f"""select name from tabSupplier
        where name = %s or {SUPPLIER_ID_FIELD} = %s """,
        (product_dict.get("vendor"), product_dict.get("vendor").lower()),
        as_list=1,
      )

      if supplier:
        return product_dict.get("vendor")
      supplier = frappe.get_doc(
        {
          "doctype": "Supplier",
          "supplier_name": product_dict.get("vendor"),
          SUPPLIER_ID_FIELD: product_dict.get("vendor").lower(),
          "supplier_group": self._get_supplier_group(),
        }
      ).insert()
      return supplier.name
    else:
      return ""

  def _get_supplier_group(self):
    supplier_group = frappe.db.get_value("Supplier Group", _("Shopify Supplier"))
    if not supplier_group:
      supplier_group = frappe.get_doc(
        {"doctype": "Supplier Group", "supplier_group_name": _("Shopify Supplier")}
      ).insert()
      return supplier_group.name
    return supplier_group

  def _create_item_supplier(self, item_code, supplier_docname, price):
    """Create Item Supplier child record if not exists."""
    supplier_fields = {
      "doctype": "Item Supplier",
      "parenttype": "Item",
      "parent": item_code,
      "supplier": supplier_docname,
      "custom_price": price,
      "main_vendor": 1,
    }
    # Remove None values
    supplier_fields = {k: v for k, v in supplier_fields.items() if v is not None}
    try:
      exists = frappe.db.exists("Item Supplier", {"parent": item_code, "supplier": supplier_docname})
      if not exists:
        item_doc = frappe.get_doc("Item", item_code)
        item_doc.append("supplier_items", supplier_fields)
        item_doc.save(ignore_permissions=True)
    except Exception:
      pass


def _add_weight_details(product_dict):
  variants = product_dict.get("variants")
  if variants:
    product_dict["weight"] = variants[0]["weight"]
    product_dict["weight_unit"] = variants[0]["weight_unit"]


def _has_variants(product_dict) -> bool:
  options = product_dict.get("options")
  return bool(options and "Default Title" not in options[0]["values"])


def _get_sku(product_dict):
  if product_dict.get("variants"):
    return product_dict.get("variants")[0].get("sku")
  return ""


def _get_item_image(product_dict):
  if product_dict.get("image"):
    return product_dict.get("image").get("src")
  return None


def _match_sku_and_link_item(item_dict, product_id, variant_id, variant_of=None, has_variant=False) -> bool:
  """Tries to match new item with existing item using Shopify SKU == item_code.

  Returns true if matched and linked.
  """
  sku = item_dict["sku"]
  if not sku or variant_of or has_variant:
    return False

  item_name = frappe.db.get_value("Item", {"item_code": sku})
  if item_name:
    try:
      ecommerce_item = frappe.get_doc(
        {
          "doctype": "Ecommerce Item",
          "integration": MODULE_NAME,
          "erpnext_item_code": item_name,
          "integration_item_code": product_id,
          "has_variants": 0,
          "variant_id": cstr(variant_id),
          "sku": sku,
        }
      )

      ecommerce_item.insert()
      return True
    except Exception:
      return False


def create_items_if_not_exist(order):
  """
  Check items from a Shopify order. If an item doesn't exist in ERPNext, create it.
  If it exists but has a mismatched Product ID or Variant ID, update the existing record.
  """
  for line_item in order.get("line_items", []):
    try:
      product_id = line_item.get("product_id")
      variant_id = line_item.get("variant_id")
      sku = line_item.get("sku")

      if not sku:
        create_shopify_log(
          message=f"Skipping item sync because SKU is missing. Item: {line_item.get('title')}",
          status="Warning",
          request_data=line_item,
        )
        continue

      # Check if an Ecommerce Item with this SKU already exists
      ecom_item_docname = frappe.db.get_value("Ecommerce Item", {"sku": sku, "integration": "shopify"}, "name")

      if not ecom_item_docname:
        # Item does not exist at all, create it
        create_shopify_log(message=f"Item with SKU '{sku}' not found. Creating new item.", status="Info", request_data=line_item)
        product = ShopifyProduct(product_id, variant_id=variant_id, sku=sku)
        product.sync_product()
      else:
        # Item exists, validate its IDs to prevent mapping failures
        ecom_item = frappe.get_doc("Ecommerce Item", ecom_item_docname)
        if str(ecom_item.integration_item_code) != str(product_id) or str(ecom_item.variant_id) != str(variant_id):
          create_shopify_log(message=f"SKU '{sku}' exists but has mismatched IDs. Updating record.", status="Info", request_data={"shopify_item": line_item, "erpnext_item_before_update": ecom_item.as_dict()})
          ecom_item.integration_item_code = str(product_id)
          ecom_item.variant_id = str(variant_id)
          ecom_item.save(ignore_permissions=True)
          frappe.db.commit()

    except Exception as e:
      create_shopify_log(message=f"Failed during item check/creation for SKU '{sku}'. Error: {e}", status="Error", exception=e, request_data=line_item)
      continue


def get_item_code(shopify_item):
  """Get item code using shopify_item dict.

  Item should contain both product_id and variant_id."""

  item = ecommerce_item.get_erpnext_item(
    integration=MODULE_NAME,
    integration_item_code=shopify_item.get("product_id"),
    variant_id=shopify_item.get("variant_id"),
    sku=shopify_item.get("sku"),
  )
  if item:
    return item.item_code

update_flags = {}
@temp_shopify_session
def upload_erpnext_item(doc, method=None):
  """This hook is called when inserting new or updating existing `Item`.

  New items are pushed to shopify and changes to existing items are
  updated depending on what is configured in "Shopify Setting" doctype.
  """
  template_item = item = doc  # alias for readability
  # a new item recieved from ecommerce_integrations is being inserted
  if item.flags.from_integration:
    return

  setting = frappe.get_doc(SETTING_DOCTYPE)

  if not setting.is_enabled() or not setting.upload_erpnext_items:
    return

  if frappe.flags.in_import:
    return

  if item.has_variants:
    return

  if len(item.attributes) > 3:
    msgprint(_("Template items/Items with 4 or more attributes can not be uploaded to Shopify."))
    return

  if doc.variant_of and not setting.upload_variants_as_items:
    msgprint(_("Enable variant sync in setting to upload item to Shopify."))
    return

  if item.variant_of:
    template_item = frappe.get_doc("Item", item.variant_of)

  import json
  product_id = frappe.db.get_value(
    "Ecommerce Item",
    {"erpnext_item_code": template_item.name, "integration": MODULE_NAME},
    "integration_item_code",
  )
  global is_new_product
  is_new_product = not bool(product_id)

  if is_new_product:
    product = Product()
    product.published = False
    product.status = "active" if setting.sync_new_item_as_active else "draft"

    map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)

    # --- Sync custom_category1–9 as Shopify metafields (overwrite existing) ---
    try:
        metafields = []
        for i in range(1, 10):
            value = getattr(item, f"custom_category{i}", None)
            metafields.append({
                "namespace": "custom",
                "key": f"layer{i}",
                "value": value or "",
                "type": "single_line_text_field"
            })
       
  
        old_metafields =getattr(product,"metafields",[]) or []
        for mf in old_metafields:
            try:
                mf.destroy()
            except Exception:
                pass
            
        product.metafields = metafields
        product.save()

        create_shopify_log(
            message=f"✅ Overwrote {len(metafields)} metafields successfully for {item.item_code}",
            status= "Success", 
            response_data=products.to_dict(),
        )
         
    except Exception as e:
        create_shopify_log(
            message=f"❌ Failed to overwrite metafields for {item.item_code}. Error: {e}",
            status="Error",
            exception=e,
            request_data={"metafields": metafields},
       )
            
    # (continue existing logic)
    if item.original_description:
        add_ai_summary(product.id, item.original_description)
    if item.original_name:
        add_ai_title(product.id, item.original_name)




    create_shopify_log(
        message=f"Creating product in Shopify: {template_item.name}",
        status="Info",
        request_data=product.to_dict(),
    )
    is_successful = product.save()

    if is_successful:
      create_shopify_log(
          message=f"Successfully created product in Shopify: {product.id}",
          status="Success",
          exception=product.errors.full_messages() or None,
          response_data=product.to_dict(),
      )
      # Determine the weight to be synced to Shopify
      weight_to_sync = max(template_item.weight_per_unit or 0, template_item.volumetric_weight or 0)

      update_default_variant_properties(
        product,
        sku=template_item.item_code,
        is_stock_item=template_item.is_stock_item,
        price=item.get(ITEM_SELLING_RATE_FIELD),
        weight=weight_to_sync,
        weight_unit=get_shopify_weight_uom(erpnext_weight_uom=template_item.weight_uom) if template_item.weight_uom else None
      )

# TODO: Fallback when there is no main vendor in Item Supplier, use the first Item Supplier in upload_erpnext_item as vendor for the shopify product

      # The product object is already up-to-date. Reloading it is redundant and can cause issues with stale data.

      # Re-map all properties, including attached images, before the final save.
      map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)

      if item.variant_of:
        product.options = []
        product.variants = []
        variant_attributes = {
          "title": template_item.item_name,
          "sku": item.item_code,
          "price": item.get(ITEM_SELLING_RATE_FIELD)
        }
        max_index_range = min(3, len(template_item.attributes))
        for i in range(0, max_index_range):
          attr = template_item.attributes[i]
          product.options.append(
            {
              "name": attr.attribute,
              "values": frappe.db.get_all(
                "Item Attribute Value", {"parent": attr.attribute}, pluck="attribute_value"
              ),
            }
          )
          try:
            variant_attributes[f"option{i+1}"] = item.attributes[i].attribute_value
          except IndexError:
            frappe.throw(
              _("Shopify Error: Missing value for attribute {}").format(attr.attribute)
            )
        product.variants.append(Variant(variant_attributes))

      create_shopify_log(
          message=f"Updating product with variants in Shopify: {template_item.name}",
          status="Info",
          request_data=product.to_dict(),
      )
      is_successful = product.save()  # push variant
      create_shopify_log(
          message=f"Successfully updated product with variants in Shopify: {product.id}",
          status="Success" if is_successful else "Error",
          exception=product.errors.full_messages() or None,
          response_data=product.to_dict(),
      )
      ecom_items = list(set([item, template_item]))
      for d in ecom_items:
        ecom_item = frappe.get_doc(
          {
            "doctype": "Ecommerce Item",
            "erpnext_item_code": d.name,
            "integration": MODULE_NAME,
            "integration_item_code": str(product.id),
            "variant_id": "" if d.has_variants else str(product.variants[0].id),
            "sku": "" if d.has_variants else str(product.variants[0].sku),
            "has_variants": d.has_variants,
            "variant_of": d.variant_of,
          }
        )
        ecom_item.insert()

      # Add product to collections based on breadcrumb
      add_product_to_collections_from_breadcrumb(product.id, item)

      # Add AI summary and title
      if item.original_description:
        add_ai_summary(product.id, item.original_description)
      if item.original_name:
        add_ai_title(product.id, item.original_name)
      
    if item.original_name:
        add_ai_title(product.id, item.original_name)
    write_upload_log(status=is_successful, product=product, item=item)
  elif setting.update_shopify_item_on_update:
    try:
      global update_flags
      update_flags = json.loads(item.update_flags or '{}')
    except (json.JSONDecodeError, TypeError):
      # Fallback to default if JSON is invalid
      update_flags = {
        'update_product': True,
        'update_default_variant': True,
        'update_variant': True,
        'update_collections': True,
        'update_metafields': True,
        'update_images': True,
      }

    # Replace the existing Product.find() call with find_or_create_product()
    product = find_or_create_product(product_id, template_item)
    is_successful = False
    variant_to_update = None

    if product:
      # Save product-level fields first (title, category, etc.)
      map_erpnext_item_to_shopify(shopify_product=product, erpnext_item=template_item)
      create_shopify_log(
          message=f"Updating product in Shopify: {product.id}",
          status="Info",
          request_data=product.to_dict(),
      )
      is_successful = product.save()
     
      metafields = []
      for i in range(1, 10):
          value = getattr(item, f"custom_category{i}", None)
          if value:
              metafields.append({
                  "namespace": "custom",
                  "key": f"layer{i}",
                  "value": value,
                  "type": "single_line_text_field"
              })

      if metafields:
          product.metafields = metafields
          product.save()
          frappe.logger().info(f"Synced {len(metafields)} metafields to Shopify for {item.item_code}")


      create_shopify_log(
          message=f"Product update response from Shopify: {product.id}",
          status="Success" if is_successful else "Error",
          exception=product.errors.full_messages() or None,
          response_data=product.to_dict(),
      )

      if is_successful:
        # Only update collections if flag is set
        if update_flags.get('update_collections'):
          add_product_to_collections_from_breadcrumb(product.id, item)
        # Only update metafields if flag is set
        if update_flags.get('update_metafields'):
          # Add AI summary and title
          if item.original_description:
            add_ai_summary(product.id, item.original_description)
          if item.original_name:
            add_ai_title(product.id, item.original_name)
      
      product.reload()

      if not item.variant_of:
        weight_to_sync = max(item.weight_per_unit or 0, item.volumetric_weight or 0)
        is_successful = update_default_variant_properties( # This function now handles the save
          product,
          is_stock_item=template_item.is_stock_item,
          sku=item.item_code, # This will be modified on the live object, not in the log
          price=item.get(ITEM_SELLING_RATE_FIELD),
          weight=weight_to_sync,
          
          weight_unit=get_shopify_weight_uom(erpnext_weight_uom=item.weight_uom) if item.weight_uom else 'kg'
        )
      elif item.variant_of:
        # This is an update for an existing variant. Find it and update its properties.
        ecom_variant_id = frappe.db.get_value("Ecommerce Item", {"erpnext_item_code": item.name}, "variant_id")
        if ecom_variant_id:
            for v in product.variants:
                if str(v.id) == str(ecom_variant_id):
                    variant_to_update = v
                    break

        if not variant_to_update:
            # Fallback to SKU match if not found by ID
            for v in product.variants:
                if v.sku == item.item_code:
                    variant_to_update = v
                    break

        if variant_to_update:
            weight_to_sync = max(item.weight_per_unit or 0, item.volumetric_weight or 0)
            variant_to_update.price = item.get(ITEM_SELLING_RATE_FIELD)
            variant_to_update.weight = weight_to_sync
            variant_to_update.weight_unit = get_shopify_weight_uom(erpnext_weight_uom=item.weight_uom) if item.weight_uom else 'kg'
            create_shopify_log(
                message=f"Updating variant {item.name} in Shopify",
                status="Info",
                request_data=variant_to_update.to_dict(),
            )
            # Explicitly save the variant to ensure changes are pushed to Shopify
            is_successful = variant_to_update.save()
            update_inventory(variant_to_update.id, 1000)
            create_shopify_log(
                message=f"Variant update response for {item.name}",
                status="Success" if is_successful else "Error",
                exception=variant_to_update.errors.full_messages() or None,
                response_data=variant_to_update.to_dict(),
            )

    write_upload_log(status=is_successful, product=product, item=item, action="Updated")


def map_erpnext_variant_to_shopify_variant(shopify_product: Product, erpnext_item, variant_attributes):
  variant_product_id = frappe.db.get_value(
    "Ecommerce Item",
    {"erpnext_item_code": erpnext_item.name, "integration": MODULE_NAME},
    "integration_item_code",
  )
  if not variant_product_id:
    for variant in shopify_product.variants:
      if (
        variant.option1 == variant_attributes.get("option1")
        and variant.option2 == variant_attributes.get("option2")
        and variant.option3 == variant_attributes.get("option3")
      ):
        variant_product_id = str(variant.id)
        if not frappe.flags.in_test:
          frappe.get_doc(
            {
              "doctype": "Ecommerce Item",
              "erpnext_item_code": erpnext_item.name,
              "integration": MODULE_NAME,
              "integration_item_code": str(shopify_product.id),
              "variant_id": variant_product_id,
              "sku": str(variant.sku),
              "variant_of": erpnext_item.variant_of,
            }
          ).insert()
        break
    if not variant_product_id:
      msgprint(_("Shopify: Couldn't sync item variant."))
  return variant_product_id


def map_erpnext_item_to_shopify(shopify_product: Product, erpnext_item):
  """Map erpnext fields to shopify, called both when updating and creating new products.
  Can accept a Product object or a dictionary."""
  # Use a setter to handle both live objects and dictionaries
  setter = setattr if isinstance(shopify_product, Product) else lambda obj, key, val: obj.setdefault(key, val)

  setter(shopify_product, 'title', erpnext_item.item_name)
  setter(shopify_product, 'body_html', erpnext_item.raw_html_description)
  setter(shopify_product, 'product_type', erpnext_item.item_group)

  main_supplier_details = frappe.db.get_value("Item Supplier", {"parent": erpnext_item.name, "main_vendor": 1}, ["supplier", "custom_price"], as_dict=1)
  if main_supplier_details:
    setter(shopify_product, 'vendor', main_supplier_details.supplier)
    # Temporarily attach price to the item doc for use in variant creation
    erpnext_item.main_vendor_price = main_supplier_details.custom_price
  else:
    erpnext_item.main_vendor_price = None

  images = []
  if erpnext_item.image:
    images.append({"src": frappe.utils.get_url(erpnext_item.image)})

  # Get attached images
  attached_files = frappe.get_all(
    "File",
    filters={"attached_to_doctype": "Item", "attached_to_name": erpnext_item.name, "is_folder": 0},
    fields=["file_url"],
  )
  for f in attached_files:
    if f.file_url not in [img["src"] for img in images]:
      images.append({"src": frappe.utils.get_url(f.file_url)})

  if images:
    setter(shopify_product, 'images', images)

  if erpnext_item.disabled:
    setter(shopify_product, 'status', "draft")
    setter(shopify_product, 'published', False)
    msgprint(_("Status of linked Shopify product is changed to Draft."))


def get_shopify_weight_uom(erpnext_weight_uom: str) -> str:
  for shopify_uom, erpnext_uom in WEIGHT_TO_ERPNEXT_UOM_MAP.items():
    if erpnext_uom == erpnext_weight_uom:
      return shopify_uom


def update_default_variant_properties(
  shopify_product: Product,
  is_stock_item: bool,
  sku: str | None = None,
  price: float | None = None,
  weight: float | None = None,
  weight_unit: str | None = None,
):
  """Shopify creates a default variant upon saving the product.

  Some item properties are supposed to be updated on the default variant.
  Input: saved shopify_product, sku and price
  """
  default_variant: Variant = Variant.find(shopify_product.variants[0].id)

  # this will create Inventory item and qty will be updated by scheduled job.
  if is_stock_item:
    default_variant.inventory_management = "shopify"

  if price is not None:
    default_variant.price = frappe.utils.flt(price)
  if sku:
    default_variant.sku = sku
  if weight is not None:
    default_variant.weight = weight
  if weight_unit is not None:
    default_variant.weight_unit = weight_unit

  create_shopify_log(
      message=f"Updating default variant for product: {shopify_product.id}",
      status="Info",
      request_data=default_variant.to_dict(),
  )
  is_successful = default_variant.save()
  update_inventory(default_variant.id, 1000)
  create_shopify_log(
      message=f"Response from default variant update for product: {shopify_product.id}",
      status="Success" if is_successful else "Error",
      exception=default_variant.errors.full_messages() or None,
      response_data=default_variant.to_dict(),
  )
  return is_successful

def write_upload_log(status: bool, product: Product, item, action="Created") -> None:
  if not status:
    msg = _("Failed to upload item to Shopify") + "<br>"
    msg += _("Shopify reported errors:") + " " + ", ".join(product.errors.full_messages())
    msgprint(msg, title="Note", indicator="orange")

    create_shopify_log(
      status="Error",
      response_data=product.to_dict(),
      exception=msg,
      message=msg,
      method="upload_erpnext_item",
    )
  else:
    create_shopify_log(
      status="Success",
      response_data=product.to_dict(),
      message=f"{action} Item: {item.name}, shopify product: {product.id}",
      method="upload_erpnext_item",
    )

def upload_product_image_to_shopify(product_id, url):
    """
    Uploads an image to a specific Shopify product using base64 encoding.

    Args:

        product_id (int): The ID of the product to upload the image to.
        image_filepath (str): The local file path to the image you want to upload.

    Returns:
        dict: The JSON response from the Shopify API, or an error dictionary.
    """
    setting = frappe.get_doc(SETTING_DOCTYPE)
    try:

        headers = get_shopify_headers(setting)
        api_version = "2024-10"
        url = f"https://{setting.shopify_url}/admin/api/{api_version}/products/{product_id}/images.json"

        payload = {"image": {"src": url}}
        create_shopify_log(message=f"Uploading image to product {product_id}", status="Info", request_data=payload)
        response = requests.post(url, headers=headers,
                                 data=json.dumps(payload))
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        frappe.log_error(f"An HTTP error occurred: {http_err}")
        return {"error": f"HTTP Error: {http_err}", "response_text": response.text}
    except Exception as err:
        frappe.log_error(f"An HTTP error occurred: {http_err}")
        return {"error": f"An unexpected error occurred: {err}"}
