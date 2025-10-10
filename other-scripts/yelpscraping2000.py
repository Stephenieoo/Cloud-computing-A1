import json
import requests
import time
from datetime import datetime, timezone
from decimal import Decimal
import boto3

def float_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: float_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [float_to_decimal(i) for i in obj]
    else:
        return obj
    

API_KEY = "W4pXfc4koP2qnCWscknvVXUh2qCJPsJNQw9t6NwPyuDbnbrOJayf6CTp3w3tQMo6Gj5WQGggzD8nQQaT8IkQCT-8FJ_DEIXZdQe3r5f-Jn50pBrLDDfwUxzKpI7iaHYx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
URL = "https://api.yelp.com/v3/businesses/search"
cuisines = ["Chinese", "Italian", "Japanese", "American", "Korean", "Thai", "French", "Mexican", "Spanish", "BBQ"]

location = "Manhattan"
LIMIT = 50  # Yelp allows max 50 per request
restaurants = {}

for cuisine in cuisines:
    for offset in range(0, 200, LIMIT):  # 200 per cuisine
        params = {
            "term": f"{cuisine} restaurants",
            "location": location,
            "limit": LIMIT,
            "offset": offset
        }
        response = requests.get(URL, headers=HEADERS, params=params)
        data = response.json()
        for biz in data.get("businesses", []):
            # print(biz) 
            restaurants[biz["id"]] = {
                "BusinessID": biz["id"],
                "Name": biz["name"],
                "Address": ", ".join(biz["location"]["display_address"]),
                "Coordinates": biz["coordinates"],
                "NumReviews": biz.get("review_count", 0),
                "Rating": biz.get("rating", 0),
                "ZipCode": biz["location"].get("zip_code", ""),
                "Cuisine": cuisine,
                "InsertedAtTimestamp": datetime.now(timezone.utc).isoformat()
            }
        time.sleep(0.5)  # avoid hitting rate limits

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("yelp-restaurants")

for item in restaurants.values():
    item = float_to_decimal(item)
    table.put_item(Item=item)

print(f"Inserted {len(restaurants)} restaurants into DynamoDB!")