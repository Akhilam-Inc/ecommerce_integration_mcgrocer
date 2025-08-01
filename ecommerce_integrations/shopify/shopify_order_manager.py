import requests


class ShopifyOrderManager:
    """
    This class, `ShopifyOrderManager`, provides a set of methods for interacting with the 
    Shopify API to manage orders. It includes functionality for creating new orders, retrieving 
    order details, updating existing orders, adding items to orders, removing items from orders,
    and updating the quantity of items in orders.

    The class takes a `shop_url` and an `access_token` in its constructor, which are used to 
    authenticate and interact with the Shopify API. The `base_url` property is constructed 
    from the `shop_url` and used as the base for all API requests.

    The methods in this class are:
    - `create_order(customer_id, line_items)`: Creates a new order for the specified
    customer with the provided line items.
    - `get_order(order_id)`: Retrieves the details of a specific order from Shopify.
    - `update_order(order_id, customer_id, line_items)`: Updates an existing order with
    the specified order ID, customer ID, and line items.
    - `add_item_to_order(order_id, variant_id, quantity)`: Adds a new line item to an 
    existing order.
    - `remove_item_from_order(order_id, line_item_id)`: Removes a specific line item 
    from an existing order.
    - `update_item_quantity(order_id, line_item_id, new_quantity)`: Updates the quantity 
    of a specific line item in an existing order.
    """

    def __init__(self, shop_url, access_token):
        self.shop_url = shop_url
        self.headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }
        self.base_url = f"https://{shop_url}/admin/api/2023-07"

    def create_order(self, customer_id, line_items):
        """
        Creates a new order for a customer.

        Args:
          customer_id (int): The ID of the customer placing the order.
          line_items (list): A list of dictionaries, each representing an item in the order.

        Returns:
          dict: The JSON response from the Shopify API containing details of the created order.
        """
        url = f"{self.base_url}/orders.json"  # Create a new order
        payload = {
            "order": {
                "customer_id": customer_id,
                "line_items": line_items
            }
        }

        response = requests.post(
            url, headers=self.headers, json=payload, timeout=10)
        return response.json()

    def get_order(self, order_id):
        """
        Retrieve the details of a specific order from Shopify.

        Args:
          order_id (str): The unique identifier of the order to retrieve.

        Returns:
          dict: A dictionary containing the order details.

        Raises:
          requests.exceptions.RequestException: If there is an issue with the HTTP request.
        """
        url = f"{self.base_url}/orders/{order_id}.json"
        response = requests.get(url, headers=self.headers, timeout=10)
        return response.json()

    def update_order(self, order_id, customer_id, line_items):
        """
        Updates an order with the given order ID, customer ID, and line items.

        Args:
          order_id (str): The ID of the order to update.
          customer_id (str): The ID of the customer associated with the order.
          line_items (list): A list of line items to update in the order.

        Returns:
          dict: The JSON response from the Shopify API containing the updated order details.
        """
        url = f"{self.base_url}/orders/{order_id}.json"

        payload = {
            "order": {
                "customer_id": customer_id,
                "line_items": line_items
            }
        }

        response = requests.put(
            url, headers=self.headers, json=payload, timeout=10)
        return response.json()

    def add_item_to_order(self, order_id, variant_id, quantity):
        """
        Adds an item to an existing order.

        Args:
          order_id (int): The ID of the order to which the item will be added.
          variant_id (int): The ID of the variant of the item to be added.
          quantity (int): The quantity of the item to be added.

        Returns:
          dict: The JSON response from the Shopify API containing the updated order details.
        """
        url = f"{self.base_url}/orders/{order_id}.json"

        payload = {
            "order": {
                "id": order_id,
                "line_items": [
                    {
                        "variant_id": variant_id,
                        "quantity": quantity
                    }
                ]
            }
        }

        response = requests.post(
            url, headers=self.headers, json=payload, timeout=10)
        return response.json()

    def remove_item_from_order(self, order_id, line_item_id):
        """
        Remove a line item from an order.

        Args:
          order_id (int): The ID of the order from which the item will be removed.
          line_item_id (int): The ID of the line item to be removed.

        Returns:
          dict: The JSON response from the Shopify API after updating the order.
        """
        url = f"{self.base_url}/orders/{order_id}.json"

        # First, get the current order
        get_response = requests.get(url, headers=self.headers, timeout=10)
        order_data = get_response.json()['order']

        # Filter out the line item to remove
        updated_line_items = [
            item for item in order_data['line_items']
            if item['id'] != line_item_id
        ]

        payload = {
            "order": {
                "id": order_id,
                "line_items": updated_line_items
            }
        }

        response = requests.put(
            url, headers=self.headers, json=payload, timeout=10)
        return response.json()

    def update_item_quantity(self, order_id, line_item_id, new_quantity):
        """
        Update the quantity of a specific line item in an order.

        Args:
            order_id (int): The ID of the order to update.
            line_item_id (int): The ID of the line item to update.
            new_quantity (int): The new quantity to set for the line item.

        Returns:
            dict: The JSON response from the Shopify API after updating the order.
        """
        url = f"{self.base_url}/orders/{order_id}.json"

        get_response = requests.get(url, headers=self.headers, timeout=10)
        order_data = get_response.json()['order']

        for item in order_data['line_items']:
            if item['id'] == line_item_id:
                item['quantity'] = new_quantity

        payload = {
            "order": {
                "id": order_id,
                "line_items": order_data['line_items']
            }
        }

        response = requests.put(
            url, headers=self.headers, json=payload, timeout=10)
        return response.json()
