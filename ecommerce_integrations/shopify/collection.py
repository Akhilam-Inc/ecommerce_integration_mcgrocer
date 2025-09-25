import frappe
import requests
import json
import ast
import time

from ecommerce_integrations.shopify.constants import SETTING_DOCTYPE
from ecommerce_integrations.shopify.utils import create_shopify_log, get_shopify_headers

def find_collection_by_title(store_name, access_token, title):
	# try:
		setting = frappe.get_doc(SETTING_DOCTYPE)
		headers = get_shopify_headers(setting)
		api_version = "2025-07"
		url = f"https://{setting.shopify_url}/admin/api/{api_version}/graphql.json"

		headers = {
			"Content-Type": "application/json",
			"X-Shopify-Access-Token": access_token
		}
		query = {
			"query": f"""
			query {{
			collections(first: 1, query: "title:{title}") {{
				edges {{
				node {{
					id
					title
					handle
					updatedAt
				}}
				}}
			}}
			}}
			"""
		}

		response = requests.post(url, json=query, headers=headers)
		if response.status_code == 200:
			data = response.json()
			edges = data.get("data", {}).get("collections", {}).get("edges", [])
			if edges:
				return edges[0]["node"]
			else:
				return None
		else:
			frappe.log_error(message=response.text, title="Shopify API Error")
	# except Exception as e:
	# 	return frappe.log_error(message=str(e), title="Shopify API Error")



def add_product_to_collection(shopify_product_id, collection_id):
    
	setting = frappe.get_doc(SETTING_DOCTYPE)
	headers = get_shopify_headers(setting)
	api_version = "2024-10"
	url = f"https://{setting.shopify_url}/admin/api/{api_version}/collects.json"

	payload = {
		"collect": {
			"product_id": shopify_product_id,
			"collection_id": collection_id.replace("gid://shopify/Collection/", "")
		}
	}

	while True:
		response = requests.post(url, headers=headers, json=payload)

		if response.status_code == 429:
			print("Rate limit hit. Retrying after a delay...")
			time.sleep(1)
			continue

		try:
			response.raise_for_status()
			create_shopify_log(
				message=f"Successfully added product {shopify_product_id} to collection {collection_id}", status="Success"
			)
			return
		except requests.HTTPError as e:
			create_shopify_log(message=f"Error adding product to collection: {response.text}", status="Error", request_data=payload)
			return


def get_or_create_collection(store_name, access_token, title, image_url=None, alt_text=None):
    setting = frappe.get_doc(SETTING_DOCTYPE)
    headers = get_shopify_headers(setting)
    api_version = "2024-10"
    url = f"https://{setting.shopify_url}/admin/api/{api_version}/graphql.json"

    # Step 1: Try to get the collection by title
    query = {
        "query": f"""
        query {{
          collections(first: 1, query: "title:{title}") {{
            edges {{
              node {{
                id
                title
                handle
              }}
            }}
          }}
        }}
        """
    }

    response = requests.post(url, json=query, headers=headers)
    if response.status_code == 200:
        data = response.json()
        edges = data.get("data", {}).get("collections", {}).get("edges", [])
        if edges:
            return {"status": "exists", "collection": edges[0]["node"]}

    # Step 2: Create the collection if not found
    mutation = {
        "query": """
        mutation CollectionCreate($input: CollectionInput!) {
          collectionCreate(input: $input) {
            userErrors {
              field
              message
            }
            collection {
              id
              title
              handle
              image {
                url
                altText
              }
            }
          }
        }
        """,
        "variables": {
            "input": {
                "title": title,
                "image": {
                    "src": image_url,
                    "altText": alt_text
                } if image_url else None
            }
        }
    }

    response = requests.post(url, json=mutation, headers=headers)
    if response.status_code == 200:
        result = response.json()
        errors = result.get("data", {}).get("collectionCreate", {}).get("userErrors", [])
        if errors:
            return {"status": "error", "errors": errors}
        collection = result["data"]["collectionCreate"]["collection"]
        return {"status": "created", "collection": collection}
    else:
        return {"status": "error", "message": response.text}


def add_product_to_collections_from_breadcrumb(shopify_product_id, erpnext_item_doc):
	"""
	Finds collections based on the item's breadcrumb and adds the product to them.

	:param shopify_product_id: The ID of the product in Shopify.
	:param erpnext_item_doc: The ERPNext Item document.
	"""
	setting = frappe.get_doc(SETTING_DOCTYPE)
	access_token = setting.get_password("password")

	breadcrumb_str = erpnext_item_doc.get("custom_website_breadcrumb")
	if not breadcrumb_str:
		return

	try:
		# Use ast.literal_eval for safely evaluating a string containing a Python literal
		collection_titles = ast.literal_eval(breadcrumb_str)
		if not isinstance(collection_titles, list):
			raise ValueError
	except (ValueError, SyntaxError):
		collection_titles = [c.strip() for c in breadcrumb_str.split(',') if c.strip()]

	if not collection_titles:
		return

	found_collections = []
	all_collections_found = True

	for title in collection_titles:
		collection = find_collection_by_title(setting.shopify_url, access_token, title)
		if collection:
			found_collections.append(collection)
		else:
			all_collections_found = False
			create_shopify_log(
				message=f"Collection '{title}' not found in Shopify for Item '{erpnext_item_doc.name}'. Product not added to collections.",
				status="Warning"
			)
			break

	if all_collections_found:
		for collection in found_collections:
			add_product_to_collection(shopify_product_id, collection.get("id"))
