"""Stream type classes for tap-talkable."""

import datetime
from urllib.parse import urlparse
from urllib.parse import parse_qs
from typing import Any, Dict, Optional

import pendulum
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_talkable.client import TalkableStream


class CampaignsStream(TalkableStream):
    name = "campaigns"
    path = "/campaigns"
    records_jsonpath = "$.result.campaigns[*]"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("slug", th.NumberType),
        th.Property("is_active", th.BooleanType),
        th.Property("appearance", th.StringType),
        th.Property("joinable_category_names", th.ArrayType(th.StringType)),
        th.Property("name", th.StringType),
        th.Property("status", th.StringType),
        th.Property("tag_names", th.ArrayType(th.StringType)),
        th.Property("new_customer", th.StringType),
        th.Property("origin_min_age", th.NumberType),
        th.Property("origin_max_age", th.NumberType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {"site_slug": self.config["site_slug"]}

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {"campaign_id": record["id"]}


MATH_METRICS = [
    'sign_up_percentage', 'advocate_pages_shown', 'advocate_impressions', 'widget_click_percentage',
    'advocacy_percentage', 'sharing_rate', 'click_rate', 'total_sales_aov', 'visit_percentage',
    'refunded_sales_percentage', 'refunded_sales_aov', 'average_order_value', 'revenue_percentage',
    'total_first_time_sales_aov', 'conversion_percentage'
]

FRIEND_SALES_METRICS = [
    'total_referred_sales_count', 'total_referred_sales_sum', 'talkable_sales_count', 'talkable_sales_sum',
    'talkable_sales_with_talkable_coupon_count', 'talkable_sales_with_talkable_coupon_sum',
    'talkable_sales_with_talkable_coupon_and_new_count', 'talkable_sales_with_talkable_coupon_and_new_sum',
    'talkable_sales_with_talkable_coupon_and_not_new_count', 'talkable_sales_with_talkable_coupon_and_not_new_sum',
    'referred_sales_new_count', 'referred_sales_new_sum', 'talkable_qualified_sales_count', 'talkable_qualified_sales_sum',
    'ideal_friend_sales_count', 'ideal_friend_sales_sum'
]

ADVOCATE_SALES_METRICS = [
    'ideal_advocate_sales_new_customers_count', 'ideal_advocate_sales_new_customers_sum',
    'ideal_advocate_sales_existing_customers_count', 'ideal_advocate_sales_existing_customers_sum',
    'ideal_advocate_sales_count', 'ideal_advocate_sales_sum'
]

PREDEFINED_METRICS = [
    'offers',
    'clicks', 'clicks_unique', 'first_time_clicks', 'visits', 'visits_unique', 'first_time_visits', 'email_gated', 'emails_collected_on_claim', 'email_gated_and_opted_in', 'email_gated_and_opted_in_new',  # Claiming
    'customers', 'new_customers', 'visited_referrals',  # Customers
    'total_first_time_sales_count', 'total_first_time_sales_sum',  # Email capture
    'total_sales_count', 'total_sales_sum', 'total_sales_with_curebit_coupon_count', 'total_sales_with_curebit_coupon_sum', 'total_sales_with_talkable_ad_coupon_count', 'total_sales_with_talkable_ad_coupon_sum', 'total_sales_with_talkable_fr_coupon_count', 'total_sales_with_talkable_fr_coupon_sum', 'lift_in_sales', 'refunded_sales_count', 'refunded_sales_sum',  # Overall sales
    'total_rewards_count', 'total_rewards_sum', 'ad_rewards_count', 'ad_rewards_sum', 'fr_rewards_count', 'fr_rewards_sum', 'advocate_loyalty_referral_rewards_count', 'advocate_loyalty_referral_rewards_sum', 'advocate_loyalty_referral_rewards_points', 'advocate_referral_rewards_used_count', 'friend_reward_paid_email_delivered', 'friend_reward_paid_email_opened', 'friend_reward_paid_email_clicked', 'advocate_reward_paid_email_delivered', 'advocate_reward_paid_email_opened', 'advocate_reward_paid_email_clicked', 'advocate_referral_rewards_paid_and_pending_count', 'advocate_referral_rewards_paid_count', # Rewards
    'offer_shown', 'sharers', 'shares', 'multisharers', 'share_emails_sent', 'share_emails_delivered', 'share_emails_opened', 'share_emails_clicked', # Sharing
    'signup_shown', 'sign_up', 'sign_up_and_opted_in', 'sign_up_and_opted_in_new', 'emails_collected_on_signup', 'people', 'advocate_offer_email_delivered', 'advocate_offer_email_opened', 'advocate_offer_email_clicked',  # Sign Up
    'ideal_sales_count', 'ideal_sales_sum', 'dashboard_sales_count', 'dashboard_sales_sum'  # Talkable Sales
]

METRICS = MATH_METRICS + FRIEND_SALES_METRICS + ADVOCATE_SALES_METRICS + PREDEFINED_METRICS


class CampaignMetricsStream(TalkableStream):
    name = "campaign_metrics"
    parent_stream_type = CampaignsStream
    ignore_parent_replication_keys = True
    path = "/metrics/{metric}/segmentize/"
    records_jsonpath = "$.result.segmented[*]"
    primary_keys = ["campaign_id", "metric", "date"]
    state_partitioning_keys = ["campaign_id", "metric"]
    replication_key = "date"
    partitions = [{"metric": metric} for metric in METRICS]

    schema = th.PropertiesList(
        th.Property("campaign_id", th.NumberType),
        th.Property("metric", th.StringType),
        th.Property("plain", th.NumberType),
        th.Property("formatted", th.StringType),
        th.Property("result_type", th.StringType),
        th.Property("period", th.StringType),
        th.Property("date", th.DateTimeType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row["date"] = datetime.datetime.strptime(row["period"].split(' ')[0], "%m/%d/%y")
        return row

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        if next_page_token is None:
            start_date = self.get_starting_timestamp(context) or th.cast(datetime.datetime, pendulum.parse(self.config["start_date"]))
        else:
            start_date = next_page_token
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = min(start_date + datetime.timedelta(days=100), yesterday)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        return {
            "site_slug": self.config["site_slug"],
            "campaign_ids": [context["campaign_id"]],
            "start_date": start_date,
            "end_date": end_date,
            "segment_by[period]": "day",
        }

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        start_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["start_date"][0],
            "%Y-%m-%d"
        )
        end_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["end_date"][0],
            "%Y-%m-%d"
        )
        if start_date.date() < end_date.date():
            next_page_token = end_date
        else:
            next_page_token = None
        return next_page_token

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 429:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)
        elif 500 <= response.status_code < 600:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)


class TrafficSourcesStream(TalkableStream):
    name = "traffic_sources"
    path = "/traffic_sources"
    records_jsonpath = "$.result[*]"
    primary_keys = ["identifier"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("identifier", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {"site_slug": self.config["site_slug"]}
