import requests

# Endpoint URL
url = "http://127.0.0.1:5003/api/daily-area-pokemon-stats"

# Your API secret keys
api_secret_key = "your_api_secret_key"
api_secret_header_key = "your_api_secret_header_key"

# Headers
headers = {
    "X-Stats-Api-Secret": api_secret_header_key
}

# Query parameters
params = {
    "secret": api_secret_key
}

# Making the GET request
response = requests.get(url, headers=headers, params=params)

# Checking the response
if response.status_code == 200:
    print("Success:", response.json())
else:
    print("Error:", response.status_code, response.text)
