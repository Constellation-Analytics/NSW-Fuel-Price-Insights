# Workflow Retention Policy

## 1. Logs
- Workflow logs (`workflow_YYYYMMDD_HHhMM.log`) will be automatically deleted after **90 days**.
- Deletions are logged and pushed to GitHub automatically by the `99.retention_policy` module.

## 2. Data
- Fact data older than **24 months** will be automatically deleted.

## 3. Process
- A dedicated `99.retention_policy` module will be called at the end of the pipeline to enforce the above rules.
- The module handles both log cleanup and database data deletion.

## 4. Notes
- Logs are stored in `data and logs/`.
- Git operations are performed via a GitHub Actions token (`GITHUB_TOKEN`).
