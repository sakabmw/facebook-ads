from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
import pandas as pd
from google.cloud import bigquery

# Credentials for Facebook Ads
adaccount_id = '{adaccount-id}'
app_id = '{app-id}'
app_secret = '{app-secret}'
access_token = '{access-token}'
FacebookAdsApi.init(app_id, app_secret, access_token)
my_account = AdAccount('act_%s' % (adaccount_id))

# Credentials for BigQuery
key_path = 'path/to/service-account.json'
client = bigquery.Client.from_service_account_json(key_path)

# BigQuery table to store the campaign data
table_id = '{project-id}.{dataset-name}.{table-name}'

# Facebook Ads data structure
fields = [
    Campaign.Field.account_id,
    Campaign.Field.ad_strategy_id,
    Campaign.Field.adbatch,
    Campaign.Field.adlabels,
    Campaign.Field.bid_strategy,
    Campaign.Field.boosted_object_id,
    Campaign.Field.brand_lift_studies,
    Campaign.Field.budget_rebalance_flag,
    Campaign.Field.budget_remaining,
    Campaign.Field.buying_type,
    Campaign.Field.can_create_brand_lift_study,
    Campaign.Field.can_use_spend_cap,
    Campaign.Field.configured_status,
    Campaign.Field.created_time,
    Campaign.Field.daily_budget,
    Campaign.Field.effective_status,
    Campaign.Field.execution_options,
    Campaign.Field.id,
    Campaign.Field.is_skadnetwork_attribution,
    Campaign.Field.issues_info,
    Campaign.Field.iterative_split_test_configs,
    Campaign.Field.last_budget_toggling_time,
    Campaign.Field.lifetime_budget,
    Campaign.Field.name,
    Campaign.Field.objective,
    Campaign.Field.pacing_type,
    Campaign.Field.promoted_object,
    Campaign.Field.recommendations,
    Campaign.Field.smart_promotion_type,
    Campaign.Field.source_campaign,
    Campaign.Field.source_campaign_id,
    Campaign.Field.special_ad_categories,
    Campaign.Field.special_ad_category,
    Campaign.Field.special_ad_category_country,
    Campaign.Field.spend_cap,
    Campaign.Field.start_time,
    Campaign.Field.status,
    Campaign.Field.stop_time,
    Campaign.Field.topline_id,
    Campaign.Field.updated_time,
    Campaign.Field.upstream_events
]

list_keys = [
    'account_id',
    'ad_strategy_id',
    'adbatch',
    'adlabels',
    'bid_strategy',
    'boosted_object_id',
    'brand_lift_studies',
    'budget_rebalance_flag',
    'budget_remaining',
    'buying_type',
    'can_create_brand_lift_study',
    'can_use_spend_cap',
    'configured_status',
    'created_time',
    'daily_budget',
    'effective_status',
    'execution_options',
    'id',
    'is_skadnetwork_attribution',
    'issues_info',
    'iterative_split_test_configs',
    'last_budget_toggling_time',
    'lifetime_budget',
    'name',
    'objective',
    'pacing_type',
    'promoted_object',
    'recommendations',
    'smart_promotion_type',
    'source_campaign',
    'source_campaign_id',
    'special_ad_categories',
    'special_ad_category',
    'special_ad_category_country',
    'spend_cap',
    'start_time',
    'status',
    'stop_time',
    'topline_id',
    'updated_time',
    'upstream_events'
]

# Creating a blank dictionary for storing the data
data = {key:[] for key in list_keys}

# Getting the data
campaigns = my_account.get_campaigns(fields=fields)

# Store the data into the dictionary
for element in campaigns:
    for key, value in data.items():
        value.append(element['%s' % (key)])

# Creating a dataframe from the dictionary
df = pd.DataFrame(data)

# Ingest the data to BigQuery
job_config = bigquery.LoadJobConfig()
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

table = client.get_table(table_id)  # Make an API request.
print(
    'Loaded {} rows and {} columns to {}'.format(
        table.num_rows, len(table.schema), table_id
    )
)
