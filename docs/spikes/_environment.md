# Environment ของ Phase 0 spikes

> คำเตือน: ห้ามใส่ secret, token, API key, password หรือ webhook URL เต็ม ๆ ในไฟล์นี้ บันทึกเฉพาะ region/host/version/id ที่ไม่เป็นความลับ

## Databricks (Free Edition)

- Workspace region: <!-- เช่น us-east-1 -->
- Workspace URL host: <!-- เฉพาะ host เช่น dbc-xxxx.cloud.databricks.com -->

## Neon (Postgres)

- Region: AWS US East 2 (Ohio), project `vulnintel-cmdb`
- Postgres version: <!-- -->

## GCP

- Project id: <!-- project ไม่ผูก billing (BigQuery sandbox) -->

## Account ฟรีที่สร้างแล้ว

- [ ] Databricks Free Edition
- [x] Neon (free plan)
- [ ] GCP project (ไม่ผูก billing, BigQuery sandbox)
- [ ] NVD API key
- [ ] healthchecks.io
- [ ] Discord webhook
- [x] GitHub repo สาธารณะ `Sorasila/Vulnintel_End_to_End_Pipeline` (เพิ่มเข้า project แล้ว)
