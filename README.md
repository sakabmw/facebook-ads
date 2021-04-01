# facebook-ads
Getting the data from the Facebook Ads and storing them in Google BigQuery tables.

This repo contains script to pull the data from Facebook Ads campaigns and store them in Google BigQuery. It actually started as a task in my current company because the digital marketing team will need to collect those campaign data. It is still on going, by the way.

Based on this reference https://developers.facebook.com/docs/marketing-api/campaign-structure/, each campaign can consist of one or more Ad Set, while each Ad Set can contain one or more Ads. As of now, I just managed to pull the campaign/the highest level. There will be a script to pull Ad Set data as well since the task is still on going.
