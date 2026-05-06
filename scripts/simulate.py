import pandas as pd
import os
import time

# ========================================
# إعدادات السكريبت
# ========================================
INPUT_FILE  = r"C:\Users\ahmed\Desktop\etl-project\data\data.csv"
OUTPUT_DIR  = r"C:\Users\ahmed\Desktop\etl-project\batches"
BATCH_SIZE  = 500
SLEEP_TIME  = 5

# إنشاء فولدر الـ Batches لو مش موجود
os.makedirs(OUTPUT_DIR, exist_ok=True)

# قراءة الداتا
print("📂 بيتم تحميل الداتا...")
df = pd.read_csv(INPUT_FILE, encoding="ISO-8859-1")
print(f"✅ تم تحميل {len(df):,} صف")
print(f"📦 هيتقسم لـ {len(df) // BATCH_SIZE + 1} Batches\n")

# تقسيم وإرسال الـ Batches
batch_num = 1

for start in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[start : start + BATCH_SIZE]
    filename = os.path.join(OUTPUT_DIR, f"batch_{batch_num:03d}.csv")
    batch.to_csv(filename, index=False)
    print(f"🚀 Batch {batch_num:03d} → {len(batch)} صف → تم الحفظ ✅")
    time.sleep(SLEEP_TIME)
    batch_num += 1

print(f"\n🎉 خلص! اتعملت {batch_num - 1} Batches في {OUTPUT_DIR}")