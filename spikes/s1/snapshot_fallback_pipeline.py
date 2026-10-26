"""Spike S1: SDP pipeline สร้าง SCD2 จาก snapshot ของ bronze ด้วย create_auto_cdc_from_snapshot_flow.

เป็น source ของ pipeline `spike_s1_snapshot` (catalog vulnintel_dev, schema silver) -> silver.spike_dim_asset
next_snapshot(latest_version) คืน (DataFrame, snapshot_id) ของ snapshot ถัดไปที่ใหญ่กว่า latest_version หรือ None
(latest_version เป็น None ในการเรียกครั้งแรก; ถ้าเรียกครั้งแรกแล้วได้ None pipeline update จะล้มตาม docs)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_TABLE = "vulnintel_dev.bronze.cmdb_assets_snapshot"

# คอลัมน์ที่ Auto Loader เติมเข้า bronze ไม่ใช่ข้อมูลของ asset: ตัดออกก่อนเข้า SCD2
# ไม่งั้นทุก snapshot จะดูเหมือน "เปลี่ยน" ทุกแถว (_load_dttm, _file_path ต่างกันตลอด)
BRONZE_METADATA_COLUMNS = [
    "_rescued_data",
    "_load_dttm",
    "_file_path",
    "_filename",
    "_file_size",
    "_file_modification_time",
    "snapshot_id",
]


def next_snapshot(latest_version):
    bronze = spark.read.table(BRONZE_TABLE)  # noqa: F821 (spark ถูก inject โดย pipeline runtime)
    unseen = bronze if latest_version is None else bronze.filter(F.col("snapshot_id") > F.lit(latest_version))
    next_id = unseen.agg(F.min("snapshot_id")).collect()[0][0]
    if next_id is None:
        return None
    snapshot = bronze.filter(F.col("snapshot_id") == F.lit(next_id)).drop(*BRONZE_METADATA_COLUMNS)
    return snapshot, int(next_id)


dp.create_streaming_table("spike_dim_asset")

dp.create_auto_cdc_from_snapshot_flow(
    target="spike_dim_asset",
    source=next_snapshot,
    keys=["id"],
    stored_as_scd_type=2,
)
