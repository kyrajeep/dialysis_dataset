import requests
import random
import json

def get_random_sample(n=100, app_token=None):
    """
    Fetch n random records from the CMS dialysis dataset.
    Uses the data-api endpoint with pagination.
    """
    base_url = "https://data.cms.gov/data-api/v1/dataset/f8610e87-ba25-43a3-a49e-927dbc8701ae/data"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    # Fetch initial request to determine total count
    try:
        resp = requests.get(base_url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        # Check structure and extract total count
        if isinstance(data, dict) and "total" in data:
            total = data["total"]
        elif isinstance(data, list):
            total = len(data)
        else:
            print("Unexpected response structure:")
            print(json.dumps(data, indent=2)[:500])
            return []
        
        # If dataset is small, just return all records
        if total <= n:
            return data if isinstance(data, list) else data.get("data", [])
        
        # Generate random offsets and fetch samples
        offsets = random.sample(range(total), min(n, total))
        results = []
        
        for offset in offsets:
            try:
                r = requests.get(base_url, params={"offset": offset, "limit": 1}, headers=headers, timeout=10)
                r.raise_for_status()
                item = r.json()
                if isinstance(item, list) and len(item) > 0:
                    results.append(item[0])
                elif isinstance(item, dict):
                    results.append(item)
            except Exception as e:
                print(f"Failed to fetch offset {offset}: {e}")
                continue
        
        return results
    
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return []


if __name__ == "__main__":
    print("Fetching 100 random samples from CMS dialysis dataset...")
    sample = get_random_sample(100)
    
    print(f"\nRetrieved {len(sample)} records")
    if sample:
        print(f"\nFirst record:")
        print(json.dumps(sample[0], indent=2))
