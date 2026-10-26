# Environment ของ Phase 0 spikes

> คำเตือน: ห้ามใส่ secret, token, API key, password หรือ webhook URL เต็ม ๆ ในไฟล์นี้ บันทึกเฉพาะ region/host/version/id ที่ไม่เป็นความลับ

## Databricks (Free Edition)

- Workspace region: (Free Edition ไม่แสดง ตรวจภายหลังถ้าจำเป็น)
- Workspace URL host: `dbc-9895c944-aa73.cloud.databricks.com`

## Neon (Postgres)

- Region: AWS US East 2 (Ohio), project `vulnintel-cmdb`
- Postgres version: 18.6 (`SELECT version()` จาก spike S1, 2026-10-26)

## GCP

- Project id: `custom-dominion-472909-u5` (ไม่ผูก billing, BigQuery sandbox)

## Account ฟรีที่สร้างแล้ว

- [x] Databricks Free Edition
- [x] Neon (free plan)
- [x] GCP project (ไม่ผูก billing, BigQuery sandbox)
- [ ] NVD API key
- [ ] healthchecks.io
- [ ] Discord webhook
- [x] GitHub repo สาธารณะ `Sorasila/Vulnintel_End_to_End_Pipeline` (เพิ่มเข้า project แล้ว)
