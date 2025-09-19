import requests

def find_collection_by_title(store_name, access_token, title):
    url = f"https://{store_name}.myshopify.com/admin/api/2023-07/graphql.json"
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
            return f"No collection found with title '{title}'."
    else:
        return f"Error {response.status_code}: {response.text}"

import requests



def get_or_create_collection(store_name, access_token, title, image_url=None, alt_text=None):
    # Shopify GraphQL endpoint
    url = f"https://{store_name}.myshopify.com/admin/api/2023-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token
    }

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
