import pandas as pd
import os
import time

# ========================================
# Script Configuration
# ========================================
INPUT_FILE = r"C:\Users\ahmed\Desktop\etl-project\data\data.csv"
OUTPUT_DIR = r"C:\Users\ahmed\Desktop\etl-project\data\batches"
BATCH_SIZE = 500
SLEEP_TIME = 5

# Create batches directory if it does not exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load dataset
print("Loading dataset...")
df = pd.read_csv(INPUT_FILE, encoding="ISO-8859-1")

print(f"Loaded {len(df):,} rows")
print(f"Dataset will be split into {len(df) // BATCH_SIZE + 1} batches\n")

# Split dataset into batches
batch_num = 1

for start in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[start:start + BATCH_SIZE]

    filename = os.path.join(
        OUTPUT_DIR,
        f"batch_{batch_num:03d}.csv"
    )

    batch.to_csv(filename, index=False)

    print(
        f"Batch {batch_num:03d} saved "
        f"({len(batch)} rows)"
    )

    time.sleep(SLEEP_TIME)
    batch_num += 1

print(f"\nCompleted. {batch_num - 1} batches created in {OUTPUT_DIR}")
