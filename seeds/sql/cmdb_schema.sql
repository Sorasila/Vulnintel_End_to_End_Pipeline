-- Mock CMDB schema (Neon / Postgres 17)
-- รันซ้ำได้ (idempotent): ใช้ IF NOT EXISTS ทั้งหมด
--
-- asset_id ไม่มี NOT NULL และ UNIQUE โดยตั้งใจ: ตารางนี้ต้องรับข้อมูลเสียที่ simulator ฉีดเข้ามา
-- (asset_id ว่างหรือซ้ำ) เพื่อให้ data quality check ใน silver จับได้
-- key ของ CDC/SCD2 คือ id (bigserial) ไม่ใช่ asset_id

CREATE TABLE IF NOT EXISTS public.assets (
    id              bigserial PRIMARY KEY,
    asset_id        text,
    hostname        text,
    environment     text,
    business_unit   text,
    criticality     int,
    internet_facing boolean,
    owner_team      text,
    status          text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.asset_software (
    id           bigserial PRIMARY KEY,
    asset_id     text,
    vendor       text,
    product      text,
    version      text,
    installed_at timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- DEFAULT = ส่งเฉพาะ primary key ใน old tuple ของ UPDATE/DELETE ซึ่งพอสำหรับ connector (keys = id)
ALTER TABLE public.assets         REPLICA IDENTITY DEFAULT;
ALTER TABLE public.asset_software REPLICA IDENTITY DEFAULT;
