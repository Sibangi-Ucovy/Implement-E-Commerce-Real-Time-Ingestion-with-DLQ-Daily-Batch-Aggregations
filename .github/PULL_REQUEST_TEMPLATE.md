## 📌 Jira Ticket / Issue Reference
- Related Jira Ticket: `[DATA] Build End-to-End Real-Time & Batch Data Platform on GCP`

## 🛠️ Summary of Changes
- Added GitHub Actions CI automation workflow (`.github/workflows/ci.yml`)
- Added automated pytest execution for Apache Beam streaming unit tests
- Added automated Terraform syntax and formatting validation

## 🧪 Testing Strategy
- [x] Tested Apache Beam validation function locally with `TestPipeline`
- [x] Verified poison pill routing to DLQ (`TAG_DLQ`)
- [x] Verified Terraform HCL formatting

## ✅ Pre-Merge Checklist
- [x] Branch is up to date with `main`
- [x] No sensitive secrets or credentials committed (verified `.gitignore`)
- [x] CI pipeline runs and passes all checks
