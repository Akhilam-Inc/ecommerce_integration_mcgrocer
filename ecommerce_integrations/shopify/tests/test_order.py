# Copyright (c) 2021, Frappe and Contributors
# See LICENSE

import json
import frappe
from frappe.tests.utils import FrappeTestCase

from ecommerce_integrations.shopify.order import get_shipping_country, get_shipping_title, get_shipping_minimum_delivery_days


class TestOrder(FrappeTestCase):
	title = "Royal Mail (Standard Delivery)"
	country = "India"
	shopify_order = json.loads("""
		{
			"shipping_address": {
					"country": "India",
					"country_code": "IN",
					"province": "Gujarat",
					"province_code": "GJ"
			},
			"shipping_lines": [
				{
					"carrier_identifier": "a8d3f2627e998fa88786188bf6b81331",
					"code": "rm-standard-asia",
					"title": "Royal Mail (Standard Delivery)"
				}
			]
		}
	""")
	@classmethod # Add a class method for setup
	def setUpClass(cls):
			super().setUpClass()

	def test_sync_with_variants(self):
		pass

	def test_get_shipping_title(self):
		shopify_order_str = self.shopify_order
		order = json.loads(shopify_order_str)
		self.assertEqual(get_shipping_title(order), self.title)
	
	def test_get_shipping_minimum_delivery_days(self):
		delivery_days = get_shipping_minimum_delivery_days(self.title, self.country)
		self.assertEqual(delivery_days, 8)
	
	def test_get_shipping_country(self):
		shipping_country = get_shipping_country(self.shopify_order)
		self.assertEqual(shipping_country, self.country)
	
