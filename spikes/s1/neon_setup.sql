-- Spike S1 (ส่วน CDC): เตรียม Neon สำหรับ Lakeflow Connect PostgreSQL connector
--
-- *** รันไฟล์นี้ "เฉพาะเมื่อ" เมนู Data ingestion ใน Databricks แสดง PostgreSQL connector แล้วเท่านั้น ***
-- เพราะ Neon logical replication (ข้อ 0) เปิดแล้วเปลี่ยนกลับไม่ได้ ดู docs/spikes/S1.md ส่วน A
--
-- 0. [Neon console] Project vulnintel-cmdb -> Settings -> Logical Replication -> Enable
--    (project จะ restart compute; เปิดแล้วปิดไม่ได้)
-- 1. รัน seeds/sql/cmdb_schema.sql และ `python spikes/s1/neon_cmdb.py --case seed` ให้มีตาราง/ข้อมูลก่อน
-- 2. ส่วน A: รันด้วย connection ของ owner role (เช่น neondb_owner) ผ่าน Neon SQL Editor หรือ psql
-- 3. ส่วน B: รันด้วย connection ของ role databricks_replication (host แบบ direct ไม่มี -pooler)
--    เพราะ slot ต้องสร้างโดย role ที่ connector จะใช้
-- อย่า commit password จริงลง repo: แทน <CHOOSE_A_STRONG_PASSWORD> ตอนรันเท่านั้น

-- ===== ส่วน A (owner role) =====

SHOW wal_level;   -- ต้องได้ 'logical' (ถ้าไม่ใช่ แปลว่า step 0 ยังไม่สำเร็จ ห้ามทำต่อ)

-- ถ้า CREATE ROLE ... REPLICATION ถูกปฏิเสธ ให้สร้าง role ชื่อ databricks_replication ผ่าน Neon console
-- (Roles & Databases) แทน แล้วข้ามบรรทัด CREATE ROLE
CREATE ROLE databricks_replication WITH LOGIN REPLICATION PASSWORD '<CHOOSE_A_STRONG_PASSWORD>';

GRANT CONNECT ON DATABASE neondb TO databricks_replication;  -- แก้ neondb ถ้าใช้ชื่อ database อื่น
GRANT USAGE  ON SCHEMA public TO databricks_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO databricks_replication;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO databricks_replication;

CREATE PUBLICATION databricks_publication FOR TABLE public.assets, public.asset_software;

-- ===== ส่วน B (เชื่อมต่อในฐานะ databricks_replication) =====

SELECT pg_create_logical_replication_slot('databricks_slot', 'pgoutput');

-- ตรวจ
SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots;
SELECT pubname FROM pg_publication;

-- ===== Cleanup หลังจบ spike (สำคัญ: slot ที่ไม่มีใครอ่านจะสะสม WAL) =====
-- ต้องหยุด/ลบ ingestion pipeline และ gateway ใน Databricks ก่อน ไม่งั้น slot ยัง active และลบไม่ได้
-- SELECT pg_drop_replication_slot('databricks_slot');
-- DROP PUBLICATION IF EXISTS databricks_publication;
-- DROP OWNED BY databricks_replication;
-- DROP ROLE IF EXISTS databricks_replication;
