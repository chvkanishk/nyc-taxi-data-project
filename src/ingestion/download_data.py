"""
Script to download NYC Taxi data
"""
import requests
import os
from pathlib import Path

def download_file(url, destination):
    """
    Download file from URL to destination
    
    Args:
        url (str): URL to download from
        destination (str): Local path to save file
    """
    print(f"Downloading from {url}...")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    
    # Download with progress
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    
    with open(destination, 'wb') as file:
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                downloaded += len(chunk)
                # Show progress
                percent = (downloaded / total_size) * 100 if total_size > 0 else 0
                print(f"\rProgress: {percent:.1f}%", end='')
    
    print(f"\n✅ Downloaded to {destination}")

if __name__ == "__main__":
    # Base URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # Download 3 months of Yellow Taxi data
    months = ["2024-01", "2024-02", "2024-03"]
    
    for month in months:
        filename = f"yellow_tripdata_{month}.parquet"
        url = base_url + filename
        destination = f"data/raw/{filename}"
        
        # Check if already downloaded
        if os.path.exists(destination):
            print(f"⏭️  {filename} already exists, skipping...")
            continue
        
        download_file(url, destination)
    
    print("\n🎉 All downloads complete!")