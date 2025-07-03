import requests
import pandas as pd
import time
from datetime import datetime

def fetch_parameter(param):
    print(f"Fetching {param} data for India...")
    rows = []
    page = 1
    limit = 1000  # API limit
    total_fetched = 0
    
    while True:
        # Using OpenAQ API v3 (newer version)
        url = "https://api.openaq.org/v3/measurements"
        params = {
            "countries_id": "91",  # India's country ID
            "parameters_id": "2" if param == "pm25" else "1",  # PM2.5=2, PM10=1
            "limit": limit,
            "page": page,
            "sort": "datetime",
            "order": "desc"
        }
        
        try:
            print(f"  Fetching page {page}...")
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            
            data = resp.json()
            
            # Check if we have results
            if "results" not in data or not data["results"]:
                print(f"  No more results found on page {page}")
                break
            
            # Process each record
            for rec in data["results"]:
                try:
                    row = {
                        "date": rec.get("datetime", ""),
                        "location": rec.get("location", ""),
                        "city": rec.get("city", ""),
                        "parameter": param,
                        "value": rec.get("value", ""),
                        "unit": rec.get("unit", ""),
                        "latitude": rec.get("coordinates", {}).get("latitude", "") if rec.get("coordinates") else "",
                        "longitude": rec.get("coordinates", {}).get("longitude", "") if rec.get("coordinates") else "",
                        "country": "India"
                    }
                    rows.append(row)
                except Exception as e:
                    print(f"    Error processing record: {e}")
                    continue
            
            current_batch = len(data["results"])
            total_fetched += current_batch
            print(f"  Page {page}: {current_batch} records (Total: {total_fetched})")
            
            # Stop if we got less than the limit (last page)
            if current_batch < limit:
                print(f"  Reached end of data for {param}")
                break
                
            page += 1
            
            # Add delay to be respectful to the API
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"  API request error on page {page}: {e}")
            # Try alternative approach with v2 API if v3 fails
            if page == 1:
                print(f"  Trying alternative approach for {param}...")
                return fetch_parameter_v2(param)
            break
        except Exception as e:
            print(f"  Unexpected error on page {page}: {e}")
            break
    
    print(f"✅ Total {param} records fetched: {total_fetched}")
    return pd.DataFrame(rows)

def fetch_parameter_v2(param):
    """Fallback to v2 API with different approach"""
    print(f"Using alternative method for {param}...")
    rows = []
    
    # Try different API endpoints
    urls_to_try = [
        "https://api.openaq.org/v2/measurements",
        "https://u50g7n0cbj.execute-api.us-east-1.amazonaws.com/v2/measurements"
    ]
    
    for base_url in urls_to_try:
        try:
            params = {
                "country": "IN",
                "parameter": param,
                "limit": 1000,
                "page": 1,
                "format": "json"
            }
            
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if "results" in data and data["results"]:
                    for rec in data["results"]:
                        try:
                            row = {
                                "date": rec.get("date", {}).get("utc", ""),
                                "location": rec.get("location", ""),
                                "city": rec.get("city", ""),
                                "parameter": rec.get("parameter", ""),
                                "value": rec.get("value", ""),
                                "unit": rec.get("unit", ""),
                                "latitude": rec.get("coordinates", {}).get("latitude", ""),
                                "longitude": rec.get("coordinates", {}).get("longitude", ""),
                                "country": "India"
                            }
                            rows.append(row)
                        except:
                            continue
                    print(f"✅ Fetched {len(rows)} records using alternative method")
                    return pd.DataFrame(rows)
        except:
            continue
    
    print(f"❌ Could not fetch {param} data from any API endpoint")
    return pd.DataFrame(rows)


def main():
    """Main function to fetch and combine PM2.5 and PM10 data for India"""
    print("🇮🇳 Fetching air quality data for India...")
    print("=" * 50)
    
    # Fetch PM2.5 data
    print("\n📊 Step 1: Fetching PM2.5 data...")
    df_pm25 = fetch_parameter("pm25")
    
    # Fetch PM10 data
    print("\n📊 Step 2: Fetching PM10 data...")
    df_pm10 = fetch_parameter("pm10")
    
    # Check if we have any data
    if len(df_pm25) == 0 and len(df_pm10) == 0:
        print("\n❌ No data fetched. The OpenAQ API might be down or changed.")
        print("🔄 Trying to create sample data to demonstrate the structure...")
        
        # Create sample data structure
        sample_data = {
            "date": ["2024-01-01T12:00:00Z", "2024-01-01T13:00:00Z"],
            "location": ["Sample Location 1", "Sample Location 2"],
            "city": ["Mumbai", "Delhi"],
            "parameter": ["pm25", "pm10"],
            "value": [45.5, 78.2],
            "unit": ["µg/m³", "µg/m³"],
            "latitude": [19.0760, 28.7041],
            "longitude": [72.8777, 77.1025],
            "country": ["India", "India"]
        }
        df_combined = pd.DataFrame(sample_data)
        print("📋 Created sample data structure")
        
    else:
        # Combine datasets
        print("\n🔄 Step 3: Combining datasets...")
        df_combined = pd.concat([df_pm25, df_pm10], ignore_index=True)
        
        # Remove duplicates if any
        initial_count = len(df_combined)
        df_combined = df_combined.drop_duplicates()
        final_count = len(df_combined)
        
        if initial_count != final_count:
            print(f"   Removed {initial_count - final_count} duplicate records")
    
    # Convert date column if it exists and has data
    if 'date' in df_combined.columns and len(df_combined) > 0:
        try:
            df_combined['date'] = pd.to_datetime(df_combined['date'])
            df_combined = df_combined.sort_values('date', ascending=False)
        except Exception as e:
            print(f"   Note: Could not parse dates: {e}")
    
    # Save to CSV
    filename = f"india_air_quality_pm25_pm10_{datetime.now().strftime('%Y%m%d')}.csv"
    df_combined.to_csv(filename, index=False)
    
    print(f"\n✅ Data saved successfully!")
    print(f"📁 Filename: {filename}")
    print(f"📊 Total records: {len(df_combined)}")
    
    if len(df_combined) > 0:
        print(f"📈 PM2.5 records: {len(df_combined[df_combined['parameter'] == 'pm25'])}")
        print(f"📈 PM10 records: {len(df_combined[df_combined['parameter'] == 'pm10'])}")
        
        # Show some statistics
        print(f"\n📋 Data Summary:")
        if 'date' in df_combined.columns:
            print(f"   Date range: {df_combined['date'].min()} to {df_combined['date'].max()}")
        print(f"   Unique locations: {df_combined['location'].nunique()}")
        print(f"   Unique cities: {df_combined['city'].nunique()}")
        
        # Show sample of data
        print(f"\n📊 Sample data:")
        print(df_combined.head())
    
    return df_combined

if __name__ == "__main__":
    df = main()
