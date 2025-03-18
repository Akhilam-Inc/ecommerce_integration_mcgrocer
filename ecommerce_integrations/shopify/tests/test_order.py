# Copyright (c) 2021, Frappe and Contributors
# See LICENSE

import json
import unittest

from ecommerce_integrations.shopify.order import get_shipping_title, get_shipping_minimum_delivery_days


class TestOrder(unittest.TestCase):
	
	def test_sync_with_variants(self):
		pass


	def test_get_shipping_title(self):
		shopify_order = """
		{
			"shipping_lines": [
				{
					"carrier_identifier": "a8d3f2627e998fa88786188bf6b81331",
					"code": "ups-standard",
					"current_discounted_price_set": {
						"presentment_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						},
						"shop_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						}
					},
					"discount_allocations": [],
					"discounted_price": "38.76",
					"discounted_price_set": {
						"presentment_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						},
						"shop_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						}
					},
					"id": 4987747827950,
					"is_removed": false,
					"phone": null,
					"price": "38.76",
					"price_set": {
						"presentment_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						},
						"shop_money": {
							"amount": "38.76",
							"currency_code": "GBP"
						}
					},
					"requested_fulfillment_service_id": null,
					"source": "Custom Shipping Service",
					"tax_lines": [],
					"title": "UPS (Standard)"
				}
			]
		}"""
		shopify_order_str = shopify_order
		order = json.loads(shopify_order_str)
		self.assertEqual(get_shipping_title(order), "UPS (Standard)")
	
	def test_get_shipping_minimum_delivery_days(self):
		delivery_days = get_shipping_minimum_delivery_days("Standard RM (Oceania)")
		self.assertEqual(delivery_days, 8)
	