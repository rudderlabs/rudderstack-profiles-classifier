import unittest
from unittest.mock import patch
from src.predictions.profiles_mlcorelib.py_native.attribution_report import (
    AttributionModel,
    AttributionModelRecipe,
)
import os
from ..mocks import MockModel, MockProject, MockCtx, MockMaterial


class MockWhtMaterial:
    def __init__(self) -> None:
        self.model = MockModel(None, None, None, None, None, None)
        self.base_wht_project = MockProject()
        self.wht_ctx = MockCtx()
        self.de_refs = set()

    def de_ref(self, input):
        self.de_refs.add(input)
        return MockMaterial(
            None,
            None,
            None,
            None,
            None,
        )

    def execute_text_template(self, template):
        return template


class TestAttributionModel(unittest.TestCase):
    def setUp(self) -> None:
        self.build_spec = {
            "conversion": {
                "touchpoints": [
                    {
                        "from": "inputs/rsMarketingPages",
                        "where": "4 < {{user.Var('first_invoice_amount')}}",
                    },
                    {
                        "from": "inputs/rsMarketingPages1",
                        "where": "4 < first_invoice_amount1",
                    },
                ],
                "conversion_vars": [
                    {
                        "name": "sf_order",
                        "timestamp": "user.Var('first_order_date')",
                        "conversion_window": "60d",
                    },
                    {
                        "name": "sf_order1",
                        "timestamp": "user.Var('first_order_date')",
                        "conversion_window": "60h",
                        "value": "user.Var('first_invoice_amount')",
                    },
                    {
                        "name": "sf_subscription",
                        "timestamp": "user.Var('subscription_start_date')",
                        "value": "user.Var('first_invoice_amount')",
                    },
                ],
            },
            "entity_key": "user",
            "campaign": {
                "entity_key": "campaign",
                "campaign_details": [
                    {
                        "impressions": [
                            {
                                "from": "inputs/ga_campaign_stats",
                                "date": "date",
                                "select": "sum(impressions)",
                            },
                            {
                                "from": "inputs/lkdn_ad_analytic_campaign",
                                "date": "day",
                                "select": "sum(impressions)",
                            },
                            {
                                "from": "inputs/fb_basic_campaign",
                                "date": "date",
                                "select": "sum(impressions)",
                            },
                        ]
                    },
                    {
                        "clicks": [
                            {
                                "from": "inputs/ga_campaign_stats",
                                "date": "date",
                                "select": "sum(clicks)",
                            }
                        ]
                    },
                ],
                "campaign_vars": ["google_ads_utm_campaign", "google_ads_clicks"],
                "campaign_start_date": "campaign_start_date",
                "campaign_end_date": "campaign_end_date",
            },
        }
        self.material = MockWhtMaterial()

    def test_validate(self):
        test_cases = [
            {
                "build_spec": {
                    "campaign": {"campaign_vars": [], "campaign_details": []}
                },
                "msg": "Validated successfully",
            },
            {
                "build_spec": {
                    "campaign": {
                        "campaign_vars": [],
                        "campaign_details": [{"cost": []}],
                    }
                },
                "msg": "Validated successfully",
            },
            {
                "build_spec": {
                    "campaign": {
                        "campaign_vars": ["cost1", "cost2", "cost3"],
                        "campaign_details": [{"cost1": []}, {"cost3": []}],
                    }
                },
                "msg": "campaign_vars and campaign_details have same var: cost1. Please provide unique vars in campaign_vars and campaign_details.",
            },
        ]
        for case in test_cases:
            with self.subTest(case=case["build_spec"]):
                model = AttributionModel(case["build_spec"], 1, "")
                _, msg = model.validate()
                self.assertEqual(msg, case["msg"])

    @patch("profiles_rudderstack.logger.Logger")
    def test_sql_template(self, mockLogger):
        unittest.TestCase.maxDiff = None
        recipe = AttributionModelRecipe(self.build_spec, mockLogger)
        recipe.register_dependencies(self.material)
        directory = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(directory, "output.sql"), "r") as file:
            # Read the contents of the file
            expected_sql = file.read()
            self.assertEqual(expected_sql, recipe.sql)
        self.assertEqual(
            self.material.de_refs,
            set(
                [
                    "inputs/rsMarketingPages",
                    "inputs/ga_campaign_stats",
                    "inputs/lkdn_ad_analytic_campaign",
                    "inputs/fb_basic_campaign",
                    "inputs/rsMarketingPages/var_table",
                    "entity/campaign/campaign_end_date",
                    "inputs/ga_campaign_stats/var_table",
                    "inputs/ga_campaign_stats/var_table/campaign_def_id",
                    "inputs/fb_basic_campaign/var_table/campaign_def_id",
                    "entity/campaign/campaign_start_date",
                    "inputs/lkdn_ad_analytic_campaign/var_table",
                    "user/all/var_table",
                    "entity/campaign/google_ads_clicks",
                    "entity/campaign/google_ads_utm_campaign",
                    "inputs/rsMarketingPages/var_table/user_abc_id",
                    "inputs/fb_basic_campaign/var_table",
                    "inputs/lkdn_ad_analytic_campaign/var_table/campaign_def_id",
                    "inputs/rsMarketingPages/var_table/campaign_def_id",
                    "inputs/rsMarketingPages1",
                    "inputs/rsMarketingPages1/var_table",
                    "inputs/rsMarketingPages1/var_table/user_abc_id",
                    "inputs/rsMarketingPages1/var_table/campaign_def_id",
                ]
            ),
        )

    @patch("profiles_rudderstack.logger.Logger")
    def test_invalid_conversion_window(self, mockLogger):
        self.build_spec["conversion"]["conversion_vars"][0]["conversion_window"] = "60"
        recipe = AttributionModelRecipe(self.build_spec, mockLogger)
        with self.assertRaises(ValueError) as context:
            recipe.register_dependencies(self.material)
        self.assertEqual(
            str(context.exception),
            "Invalid conversion window format. Use formats like 30m, 2h, 7d.",
        )
