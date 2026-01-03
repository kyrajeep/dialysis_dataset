import requests
import random
import json

def get_random_sample(n, app_token=None):
    """
    Fetch n random records from the CMS dialysis dataset.
    Uses the data-api endpoint with pagination.

    Note:
        This function assumes the API response may include a "total" key indicating the total number of records.
        If the API does not provide a "total" field, it falls back to using the length of the returned list.
    """
    base_url = "https://data.cms.gov/data-api/v1/dataset/f8610e87-ba25-43a3-a49e-927dbc8701ae/data"
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    # Fetch initial request to determine total count
    try:
        # Get initial response to determine total count
        resp = requests.get(base_url, params={"limit": 1}, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        # Try to get total count from header or data structure
        total = None
        if "X-Total-Count" in resp.headers:
            try:
                total = int(resp.headers["X-Total-Count"])
            except Exception:
                total = None
        if total is None:
            if isinstance(data, dict) and "total" in data:
                total = data["total"]
            elif isinstance(data, list):
                total = len(data)
            elif isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                total = len(data["data"])
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
    print("Fetching data from CMS dialysis dataset...")
    
    # Get total count
    base_url = "https://data.cms.gov/data-api/v1/dataset/f8610e87-ba25-43a3-a49e-927dbc8701ae/data"
    try:
        resp = requests.get(base_url, params={"limit": 1}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        total = None
        if "X-Total-Count" in resp.headers:
            try:
                total = int(resp.headers["X-Total-Count"])
            except Exception:
                pass
        
        if total is None:
            if isinstance(data, dict) and "total" in data:
                total = data["total"]
        
        if total:
            print(f"Total entries in dataset: {total}\n")
    except Exception as e:
        print(f"Could not fetch total count: {e}\n")
    
    # Fetch 15 records
    sample = get_random_sample(n=15)
    
    print(f"Retrieved {len(sample)} records\n")
    if sample:
        print("First 15 entries:")
        for i, record in enumerate(sample, 1):
            print(f"\n--- Entry {i} ---")
            print(json.dumps(record, indent=2))
