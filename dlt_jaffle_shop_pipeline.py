import os
import time
import requests
from urllib.parse import urljoin
import dlt

# ✅ Environment configuration for performance tuning
os.environ["EXTRACT__WORKERS"] = "6"
os.environ["NORMALIZE__WORKERS"] = "2"
os.environ["LOAD__WORKERS"] = "2"
os.environ["DLT_PIPELINE__EXTRACT__FILE_ROTATION"] = "50MB"
os.environ["DLT_PIPELINE__EXTRACT__BUFFER_MAX_ITEMS"] = "10000"

# ✅ Base API URL
BASE_URL = "https://jaffle-shop.scalevector.ai"

# ✅ Pagination function with retry logic
def paginate(url, retries=3, delay=3):
    while url:
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"[Attempt {attempt + 1}] Error fetching {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise
        yield response.json()
        next_url = response.links.get("next", {}).get("url")
        url = urljoin(BASE_URL, next_url) if next_url else None

# ✅ Define DLT resources
@dlt.resource(primary_key="id", write_disposition="merge", table_name="customers", parallelized=True)
def get_customers():
    yield from paginate(f"{BASE_URL}/api/v1/customers?page=1")

@dlt.resource(primary_key="id", write_disposition="merge", table_name="orders", parallelized=False)
def get_orders():
    yield from paginate(f"{BASE_URL}/api/v1/orders?page=1")

@dlt.resource(primary_key="id", write_disposition="merge", table_name="products", parallelized=True)
def get_products():
    yield from paginate(f"{BASE_URL}/api/v1/products?page=1")

# ✅ Group resources into a source
@dlt.source
def jaffle_shop_source():
    return [get_customers(), get_orders(), get_products()]

# ✅ Run pipeline
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_pipeline",
        destination="duckdb",  # You can change to 'postgres', 'bigquery', etc.
        dataset_name="jaffle_shop_data",
        full_refresh=False
    )

    load_info = pipeline.run(jaffle_shop_source())
    print("\n✅ Load completed!")
    print("Last Normalize Info:\n", pipeline.last_trace.last_normalize_info)
    print("Load Info:\n", load_info)
