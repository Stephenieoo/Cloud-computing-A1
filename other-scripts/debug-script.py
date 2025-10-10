import json
import requests
# import boto3

# sts = boto3.client("sts")
# identity = sts.get_caller_identity()
# print(identity)

API_KEY = "W4pXfc4koP2qnCWscknvVXUh2qCJPsJNQw9t6NwPyuDbnbrOJayf6CTp3w3tQMo6Gj5WQGggzD8nQQaT8IkQCT-8FJ_DEIXZdQe3r5f-Jn50pBrLDDfwUxzKpI7iaHYx"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
URL = "https://api.yelp.com/v3/businesses/search"

LIMIT = 50
offset = 50
cuisines = ["Chinese", "Italian", "Japanese", "American", "Korean"]
location = "Manhattan"

params = {
    "term": f"{cuisines[0]} restaurants",
    "location": location,
    "limit": LIMIT,
    "offset": offset
}

response = requests.get(URL, headers=HEADERS, params=params)
data = response.json()

# save to local
with open("sample_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("Yelp response saved to sample_response.json")