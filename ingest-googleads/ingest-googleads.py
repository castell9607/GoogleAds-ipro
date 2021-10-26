#!/usr/bin/env python3
# updated to v6

import boto3
import csv
import json
import os
import sys

from dentsu_pkgs.aws_helpers import get_secret

from datetime import datetime, timedelta

from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

month = datetime.now().strftime("%B").lower()
s3 = boto3.client("s3")


if __name__ == "__main__":
    # vars;
    client = GoogleAdsClient.load_from_env()
    ga_service = client.get_service("GoogleAdsService", version="v6")
    secrets = json.loads(get_secret(os.environ["AWS_SECRETS"], os.environ["AWS_DEFAULT_REGION"]))
    google_accounts = json.loads(secrets["google_ads_accounts"])
    print(google_accounts)

    # enum objects;
    bidding_strategies = client.get_type("BiddingStrategyTypeEnum")
    campaign_types = client.get_type("AdvertisingChannelTypeEnum")
    campaign_statuses = client.get_type("CampaignStatusEnum")
    ad_types = client.get_type("AdTypeEnum")
    # print(ad_types.AdType.items())
    ad_types_dict = {t[1]: t[0] for t in ad_types.AdType.items()}

    # automated;
    if os.getenv("IS_AUTOMATED") == "True":
        if datetime.utcnow().day == 1:
            query = (
                "SELECT "
                "ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.expanded_text_ad.description, "
                "campaign.start_date, campaign.end_date, campaign.advertising_channel_type, campaign.id, campaign.name, campaign.status, campaign.bidding_strategy_type, campaign.labels, "
                "customer.id, customer.descriptive_name, customer.currency_code, "
                "metrics.average_cost, metrics.ctr, metrics.average_cpc, metrics.average_cpm, metrics.average_cpv, metrics.cost_micros, metrics.cost_per_all_conversions, metrics.cost_per_conversion, metrics.conversions, metrics.clicks, metrics.interactions, metrics.interaction_rate, metrics.impressions, metrics.video_views, metrics.conversions_from_interactions_rate, metrics.video_quartile_p100_rate, metrics.video_quartile_p75_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p25_rate, "
                "segments.date "
                "FROM ad_group_ad "
                "WHERE segments.date DURING LAST_MONTH"
            )
            suffix = "{}-{}".format(
                (datetime.utcnow() - timedelta(days=1)).strftime("%B"), (datetime.utcnow() - timedelta(days=1)).strftime("%Y")
            )
        else:
            query = (
                "SELECT "
                "ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.expanded_text_ad.description, "
                "campaign.start_date, campaign.end_date, campaign.advertising_channel_type, campaign.id, campaign.name, campaign.status, campaign.bidding_strategy_type, campaign.labels, "
                "customer.id, customer.descriptive_name, customer.currency_code, "
                "metrics.average_cost, metrics.ctr, metrics.average_cpc, metrics.average_cpm, metrics.average_cpv, metrics.cost_micros, metrics.cost_per_all_conversions, metrics.cost_per_conversion, metrics.conversions, metrics.clicks, metrics.interactions, metrics.interaction_rate, metrics.impressions, metrics.video_views, metrics.conversions_from_interactions_rate, metrics.video_quartile_p100_rate, metrics.video_quartile_p75_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p25_rate, "
                "segments.date "
                "FROM ad_group_ad "
                "WHERE segments.date DURING THIS_MONTH"
            )
            suffix = "{}-{}".format(datetime.utcnow().strftime("%B"), datetime.utcnow().strftime("%Y"))
    else:
        # to call on range set env variables START_YEAR, START_MONTH, START_DAY, END_YEAR, END_MONTH, END_DAY with months and days set to two digits strings.
        query = (
            "SELECT "
            "ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.expanded_text_ad.description, "
            "campaign.start_date, campaign.end_date, campaign.advertising_channel_type, campaign.id, campaign.name, campaign.status, campaign.bidding_strategy_type, campaign.labels, "
            "customer.id, customer.descriptive_name, customer.currency_code, "
            "metrics.average_cost, metrics.ctr, metrics.average_cpc, metrics.average_cpm, metrics.average_cpv, metrics.cost_micros, metrics.cost_per_all_conversions, metrics.cost_per_conversion, metrics.conversions, metrics.clicks, metrics.interactions, metrics.interaction_rate, metrics.impressions, metrics.video_views, metrics.conversions_from_interactions_rate, metrics.video_quartile_p100_rate, metrics.video_quartile_p75_rate, metrics.video_quartile_p50_rate, metrics.video_quartile_p25_rate, "
            "segments.date "
            "FROM ad_group_ad "
            "WHERE segments.date BETWEEN '{start_year}-{start_month}-{start_day}' "
            "AND '{end_year}-{end_month}-{end_day}'"
        ).format(
            start_year=os.environ["START_YEAR"],
            start_month=os.environ["START_MONTH"],
            start_day=os.environ["START_DAY"],
            end_year=os.environ["END_YEAR"],
            end_month=os.environ["END_MONTH"],
            end_day=os.environ["END_DAY"],
        )

        suffix = "{start_year}-{start_month}-{start_day}-to-{end_year}-{end_month}-{end_day}".format(
            start_year=os.environ["START_YEAR"],
            start_month=os.environ["START_MONTH"],
            start_day=os.environ["START_DAY"],
            end_year=os.environ["END_YEAR"],
            end_month=os.environ["END_MONTH"],
            end_day=os.environ["END_DAY"],
        )

    for account in google_accounts:
        print(account)
        # issues a search request using streaming;
        response = ga_service.search_stream(account["id"].replace("-", ""), query)
        keys = [
            "segment_date",
            "account_id",
            "account_name",
            "campaign_status",
            "bidding_strategy_type",
            "campaign_type",
            "campaign_id",
            "campaign_name",
            "labels_resource_name",
            "ad_type",
            "ad_id",
            "ad_name",
            "currency_code",
            "average_cost",
            "ctr",
            "average_cpc",
            "average_cpm",
            "average_cpv",
            "cost_micros",
            "cost_per_all_conversions",
            "cost_per_conversion",
            "conversions",
            "clicks",
            "interactions",
            "interaction_rate",
            "impressions",
            "video_views",
            "video_quartile_100_rate",
            "video_quartile_75_rate",
            "video_quartile_50_rate",
            "video_quartile_25_rate",
        ]
        rows = []
        try:
            for batch in response:
                for row in batch.results:
                    row0 = {}
                    customer = row.customer
                    campaign = row.campaign
                    metrics = row.metrics
                    segments = row.segments
                    ad_group_ad = row.ad_group_ad

                    labels = []
                    for i in range(campaign.labels.__len__()):
                        labels.append(campaign.labels.__getitem__(i))

                    # add values to {row0};
                    row0["segment_date"] = segments.date
                    row0["account_id"] = customer.id
                    row0["account_name"] = customer.descriptive_name
                    row0["campaign_status"] = campaign_statuses.CampaignStatus.items()[campaign.status][0]
                    row0["bidding_strategy_type"] = bidding_strategies.BiddingStrategyType.items()[campaign.bidding_strategy_type][0]
                    row0["campaign_type"] = campaign_types.AdvertisingChannelType.items()[campaign.advertising_channel_type][0]
                    row0["campaign_id"] = campaign.id
                    row0["campaign_name"] = campaign.name
                    row0["labels_resource_name"] = ",".join(labels)
                    row0["ad_type"] = ad_types_dict[ad_group_ad.ad.type]
                    row0["ad_id"] = ad_group_ad.ad.id
                    row0["ad_name"] = ad_group_ad.ad.name
                    row0["currency_code"] = customer.currency_code
                    row0["average_cost"] = metrics.average_cost
                    row0["ctr"] = metrics.ctr
                    row0["average_cpc"] = metrics.average_cpc
                    row0["average_cpm"] = metrics.average_cpm
                    row0["average_cpv"] = metrics.average_cpv
                    row0["cost_micros"] = metrics.cost_micros
                    row0["cost_per_all_conversions"] = metrics.cost_per_all_conversions
                    row0["cost_per_conversion"] = metrics.cost_per_conversion
                    row0["conversions"] = metrics.conversions
                    row0["clicks"] = metrics.clicks
                    row0["interactions"] = metrics.interactions
                    row0["interaction_rate"] = metrics.interaction_rate
                    row0["impressions"] = metrics.impressions
                    row0["video_views"] = metrics.video_views
                    row0["video_quartile_100_rate"] = metrics.video_quartile_p100_rate
                    row0["video_quartile_75_rate"] = metrics.video_quartile_p75_rate
                    row0["video_quartile_50_rate"] = metrics.video_quartile_p50_rate
                    row0["video_quartile_25_rate"] = metrics.video_quartile_p25_rate

                    print(row)
                    rows.append(row0)

            if not rows:
                print("Response for account: {} : {} is empty...".format(account["id"], account["country"]))
                continue

            filename = "{}-{}.csv".format(row0["account_name"], suffix)
            with open(filename, "w") as f_csv:
                writer = csv.DictWriter(f_csv, fieldnames=keys, restval="", extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

            # push file to S3 (filename.ext, bucket, object-key);
            s3.upload_file(filename, os.environ["TARGET_BUCKET"], "raw/googleads/{}_{}.csv".format(row0["account_name"], suffix))

        except GoogleAdsException as ex:
            print(f'Request with ID "{ex.request_id}" failed with status ' f'"{ex.error.code().name}" and includes the following errors:')
            for error in ex.failure.errors:
                print(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        print(f"\t\tOn field: {field_path_element.field_name}")
            sys.exit(1)
