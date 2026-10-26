#!/usr/bin/env bash
# Spike S4b: ตั้งค่า GCP ให้ GitHub Actions เข้า BigQuery sandbox ผ่าน Workload Identity Federation (OIDC)
# รันใน Cloud Shell (มี gcloud, bq, jq อยู่แล้ว) ไม่ต้องใช้ service account key
#
# ใช้งาน:
#   bash spikes/s4/gcp_setup.sh <REPO_ID>     # สร้าง/อัปเดตทุกอย่าง (รันซ้ำได้)
#   REPO_ID=<REPO_ID> bash spikes/s4/gcp_setup.sh
#   bash spikes/s4/gcp_setup.sh --cleanup      # ลบเฉพาะ dataset ของ spike (เก็บ pool/SA ไว้ใช้ต่อ)
#
# REPO_ID = numeric id ของ repo: gh api repos/Sorasila/Vulnintel_End_to_End_Pipeline --jq .id
# ใช้ repository_id (ไม่ใช่ชื่อ repo) ใน attribute condition เพราะชื่อเปลี่ยน/ต่างตัวพิมพ์ได้
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-custom-dominion-472909-u5}"
DATASET="vulnintel_spike"
BQ_LOCATION="US"
SA_NAME="vulnintel-export"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
POOL_ID="github"
PROVIDER_ID="github-oidc"
ISSUER_URI="https://token.actions.githubusercontent.com"
ALLOWED_REF="refs/heads/main"

MODE="setup"
REPO_ID="${REPO_ID:-}"
for arg in "$@"; do
  case "$arg" in
    --cleanup) MODE="cleanup" ;;
    -h | --help)
      sed -n '2,11p' "$0"
      exit 0
      ;;
    *) REPO_ID="$arg" ;;
  esac
done

log() { echo "==> $*"; }
die() {
  echo "ERROR: $*" >&2
  exit 1
}

# gcloud ที่ผูก project ทุกครั้ง (ไม่พึ่ง gcloud config ของ Cloud Shell)
gc() { gcloud --quiet --project "$PROJECT_ID" "$@"; }

# IAM แบบ eventual consistency: SA ที่เพิ่งสร้างอาจยังไม่เห็นในทันที จึงลองซ้ำสั้น ๆ
retry() {
  local attempt
  for attempt in 1 2 3 4 5; do
    if "$@"; then return 0; fi
    echo "   (attempt $attempt/5 failed, retry in 5s)" >&2
    sleep 5
  done
  return 1
}

command -v gcloud > /dev/null || die "gcloud not found (run this in Cloud Shell)"
command -v bq > /dev/null || die "bq not found (run this in Cloud Shell)"

if [ "$MODE" = "cleanup" ]; then
  log "cleanup: ลบ dataset ${PROJECT_ID}:${DATASET} (เก็บ pool/provider/service account ไว้)"
  if bq --project_id="$PROJECT_ID" show --format=none "${PROJECT_ID}:${DATASET}" > /dev/null 2>&1; then
    bq --project_id="$PROJECT_ID" rm -r -f -d "${PROJECT_ID}:${DATASET}"
    log "deleted ${DATASET}"
  else
    log "dataset ${DATASET} does not exist (nothing to do)"
  fi
  exit 0
fi

command -v jq > /dev/null || die "jq not found (needed to edit the dataset ACL)"
[[ "$REPO_ID" =~ ^[0-9]+$ ]] || die "REPO_ID must be numeric. usage: bash $0 <REPO_ID>  (gh api repos/Sorasila/Vulnintel_End_to_End_Pipeline --jq .id)"

PROJECT_NUMBER="$(gc projects describe "$PROJECT_ID" --format='value(projectNumber)')"
[[ "$PROJECT_NUMBER" =~ ^[0-9]+$ ]] || die "could not read project number for ${PROJECT_ID}"
log "project=${PROJECT_ID} number=${PROJECT_NUMBER} repo_id=${REPO_ID}"

# 1. enable APIs: ถ้า sandbox (ไม่มี billing) ปฏิเสธ API ไหน ให้หยุดทันที ซึ่งเป็นผลของ spike เอง
log "enable APIs"
if ! enable_out="$(gc services enable \
  bigquery.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  iam.googleapis.com cloudresourcemanager.googleapis.com 2>&1)"; then
  echo "$enable_out" >&2
  die "enabling APIs failed (billing/sandbox restriction?). record the message above as a spike result"
fi

# 2. dataset (US multi-region)
log "dataset ${DATASET}"
if ! bq --project_id="$PROJECT_ID" show --format=none "${PROJECT_ID}:${DATASET}" > /dev/null 2>&1; then
  bq --project_id="$PROJECT_ID" mk --dataset --location="$BQ_LOCATION" \
    --description="Phase 0 spike S4b (safe to delete)" "${PROJECT_ID}:${DATASET}"
fi

# 3. service account
log "service account ${SA_EMAIL}"
if ! gc iam service-accounts describe "$SA_EMAIL" > /dev/null 2>&1; then
  gc iam service-accounts create "$SA_NAME" --display-name="VulnIntel export (GitHub Actions to BigQuery)"
fi

# 4. roles: jobUser ระดับ project, dataEditor ระดับ dataset
log "grant roles/bigquery.jobUser on project"
retry gc projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.jobUser" --condition=None > /dev/null

log "grant roles/bigquery.dataEditor on dataset (legacy ACL role WRITER = dataEditor)"
acl_file="$(mktemp)"
trap 'rm -f "$acl_file"' EXIT
bq --project_id="$PROJECT_ID" show --format=prettyjson "${PROJECT_ID}:${DATASET}" > "$acl_file"
if jq -e --arg e "$SA_EMAIL" 'any((.access // [])[]; .userByEmail == $e and .role == "WRITER")' "$acl_file" > /dev/null; then
  log "dataset ACL already has ${SA_EMAIL}"
else
  updated="$(mktemp)"
  jq --arg e "$SA_EMAIL" '.access = ((.access // []) + [{"role": "WRITER", "userByEmail": $e}])' "$acl_file" > "$updated"
  mv "$updated" "$acl_file"
  retry bq --project_id="$PROJECT_ID" update --source "$acl_file" "${PROJECT_ID}:${DATASET}" > /dev/null
fi

# 5. workload identity pool + OIDC provider
log "workload identity pool ${POOL_ID}"
if ! gc iam workload-identity-pools describe "$POOL_ID" --location=global > /dev/null 2>&1; then
  gc iam workload-identity-pools create "$POOL_ID" --location=global --display-name="GitHub Actions"
fi

ATTRIBUTE_MAPPING="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_id=assertion.repository_id,attribute.repository_owner_id=assertion.repository_owner_id,attribute.ref=assertion.ref"
ATTRIBUTE_CONDITION="assertion.repository_id == '${REPO_ID}' && assertion.ref == '${ALLOWED_REF}'"

log "oidc provider ${PROVIDER_ID} (condition: ${ATTRIBUTE_CONDITION})"
if gc iam workload-identity-pools providers describe-oidc "$PROVIDER_ID" \
  --workload-identity-pool="$POOL_ID" --location=global > /dev/null 2>&1; then
  # มีอยู่แล้ว: อัปเดตให้ mapping/condition ตรงกับ script เสมอ (รันซ้ำด้วย REPO_ID ใหม่ได้)
  gc iam workload-identity-pools providers update-oidc "$PROVIDER_ID" \
    --workload-identity-pool="$POOL_ID" --location=global \
    --issuer-uri="$ISSUER_URI" \
    --attribute-mapping="$ATTRIBUTE_MAPPING" \
    --attribute-condition="$ATTRIBUTE_CONDITION"
else
  gc iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
    --workload-identity-pool="$POOL_ID" --location=global \
    --issuer-uri="$ISSUER_URI" \
    --attribute-mapping="$ATTRIBUTE_MAPPING" \
    --attribute-condition="$ATTRIBUTE_CONDITION"
fi

# 6. ให้ identity จาก repo นี้ impersonate service account ได้
PRINCIPAL_SET="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository_id/${REPO_ID}"
log "bind roles/iam.workloadIdentityUser to ${PRINCIPAL_SET}"
retry gc iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --role="roles/iam.workloadIdentityUser" --member="$PRINCIPAL_SET" > /dev/null

cat << SUMMARY

================ DONE ================
เพิ่ม 3 ค่านี้เป็น GitHub *Variables* (ไม่ใช่ Secrets):
repo -> Settings -> Secrets and variables -> Actions -> Variables

GCP_WIF_PROVIDER = projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}
GCP_SA_EMAIL     = ${SA_EMAIL}
GCP_PROJECT_ID   = ${PROJECT_ID}

หมายเหตุ: condition ยอมเฉพาะ ref ${ALLOWED_REF} ดังนั้น workflow ต้องรันจาก main (merge ก่อนรัน)
ลบ dataset หลังจบ spike: bash $0 --cleanup
SUMMARY
