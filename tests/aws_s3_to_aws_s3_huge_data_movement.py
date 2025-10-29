import boto3
import csv
import io
from io import BytesIO, TextIOWrapper

s3_client = boto3.client('s3')

# Configuration
SOURCE_BUCKET = 'your-bucket-name'
SOURCE_KEY = 'prefix/to/your/file.csv'  # e.g., 'data/input/large_file.csv'
DEST_BUCKET = 'your-bucket-name'  # same bucket or different
DEST_PREFIX = 'prefix/to/split/files'  # e.g., 'data/output'
CHUNK_SIZE_MB = 250
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024

def split_s3_csv():
    """
    Streams CSV from S3, splits into 250MB chunks, uploads back to S3.
    
    MEMORY USAGE: ~500MB max (not 50GB!)
    DOWNLOAD: Happens in parallel with processing
    TIME: Depends on your internet speed, not file size
    """
    
    print(f"Starting to stream {SOURCE_KEY} from S3...")
    
    # Get the object from S3 - this returns a StreamingBody, not the whole file
    response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    
    chunk_num = 0
    bytes_written = 0
    header = None
    row_count = 0
    total_rows = 0
    
    # Stream the file line by line (small memory footprint!)
    with TextIOWrapper(response['Body'], encoding='utf-8') as f:
        reader = csv.reader(f)
        writer = None
        chunk_file = None
        chunk_buffer = None
        
        for row_num, row in enumerate(reader):
            # Capture header on first row
            if row_num == 0:
                header = row
                print(f"Header detected: {len(header)} columns")
                continue
            
            # Initialize new chunk on first data row or after previous chunk is full
            if writer is None:
                chunk_buffer = BytesIO()
                chunk_file = TextIOWrapper(chunk_buffer, encoding='utf-8', newline='')
                writer = csv.writer(chunk_file)
                writer.writerow(header)
                bytes_written = 0
                row_count = 0
            
            # Write row to chunk
            writer.writerow(row)
            row_count += 1
            bytes_written += len(','.join(row).encode('utf-8')) + 1  # +1 for newline
            total_rows += 1
            
            # Print progress every 10k rows
            if total_rows % 10000 == 0:
                print(f"Processed {total_rows} rows... Current chunk: {bytes_written / 1024 / 1024:.2f} MB")
            
            # Check if chunk is full (250MB)
            if bytes_written >= CHUNK_SIZE_BYTES:
                # Finalize and upload current chunk
                chunk_file.flush()
                chunk_file.close()
                chunk_buffer.seek(0)
                
                chunk_num += 1
                chunk_key = f"{DEST_PREFIX}/chunk_{chunk_num:04d}.csv"
                chunk_size_mb = bytes_written / 1024 / 1024
                
                print(f"\n>>> Uploading {chunk_key}")
                print(f"    Size: {chunk_size_mb:.2f} MB, Rows: {row_count}")
                
                s3_client.put_object(
                    Bucket=DEST_BUCKET,
                    Key=chunk_key,
                    Body=chunk_buffer.getvalue()
                )
                
                print(f"    ✓ Upload complete\n")
                
                # Reset for next chunk
                writer = None
                chunk_file = None
                chunk_buffer = None
        
        # Upload final chunk if it has data
        if writer is not None:
            chunk_file.flush()
            chunk_file.close()
            chunk_buffer.seek(0)
            
            chunk_num += 1
            chunk_key = f"{DEST_PREFIX}/chunk_{chunk_num:04d}.csv"
            chunk_size_mb = bytes_written / 1024 / 1024
            
            print(f"\n>>> Uploading final {chunk_key}")
            print(f"    Size: {chunk_size_mb:.2f} MB, Rows: {row_count}")
            
            s3_client.put_object(
                Bucket=DEST_BUCKET,
                Key=chunk_key,
                Body=chunk_buffer.getvalue()
            )
            
            print(f"    ✓ Upload complete\n")
    
    print(f"\n{'='*50}")
    print(f"✓ COMPLETE!")
    print(f"  Total chunks: {chunk_num}")
    print(f"  Total rows: {total_rows}")
    print(f"  Location: s3://{DEST_BUCKET}/{DEST_PREFIX}/")
    print(f"{'='*50}")

if __name__ == '__main__':
    try:
        split_s3_csv()
    except Exception as e:
        print(f"Error: {e}")
        print("Check that:")
        print("  1. AWS credentials are configured")
        print("  2. SOURCE_BUCKET and SOURCE_KEY are correct")
        print("  3. You have read permission on source and write permission on destination")