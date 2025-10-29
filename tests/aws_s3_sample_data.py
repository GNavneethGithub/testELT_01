import boto3
import csv
from io import TextIOWrapper

s3_client = boto3.client('s3')

# Configuration
SOURCE_BUCKET = 'your-bucket-name'
SOURCE_KEY = 'prefix/to/your/file.csv'  # e.g., 'data/input/large_file.csv'
OUTPUT_FILE = 'sample_10_records.csv'  # will be saved to your local PC
RECORDS_TO_DOWNLOAD = 10

def download_sample_records():
    """
    Downloads first N records from S3 CSV file to local PC.
    This downloads ONLY the header + N rows, not the whole file.
    """
    
    print(f"Fetching sample from s3://{SOURCE_BUCKET}/{SOURCE_KEY}...")
    print(f"Will download: header + {RECORDS_TO_DOWNLOAD} records\n")
    
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
        
        records = []
        header = None
        
        # Stream and read only first N records
        with TextIOWrapper(response['Body'], encoding='utf-8') as f:
            reader = csv.reader(f)
            
            for row_num, row in enumerate(reader):
                if row_num == 0:
                    header = row
                    records.append(row)
                    print(f"✓ Header detected: {len(header)} columns")
                    print(f"  Columns: {', '.join(header[:5])}{'...' if len(header) > 5 else ''}\n")
                    continue
                
                if row_num <= RECORDS_TO_DOWNLOAD:
                    records.append(row)
                else:
                    break
        
        # Write to local file
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(records)
        
        print(f"✓ Downloaded {len(records)-1} records (+ header)")
        print(f"✓ Saved to: {OUTPUT_FILE}\n")
        print(f"File size: {os.path.getsize(OUTPUT_FILE) / 1024:.2f} KB")
        print(f"\nYou can now open '{OUTPUT_FILE}' with Excel or any text editor to review the data.")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nCheck that:")
        print("  1. AWS credentials are configured")
        print("  2. SOURCE_BUCKET is correct (check spelling/case)")
        print("  3. SOURCE_KEY is correct (full path to the CSV file)")
        print("  4. You have read permission on the file")

if __name__ == '__main__':
    import os
    download_sample_records()