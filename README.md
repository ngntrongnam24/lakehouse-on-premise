# 🏗️ Lakehouse On-Premise

> **Nhóm Fresher Data Engineer** — Thực hành xây dựng Lakehouse On-Premise trong 30 ngày.

## 📋 Tổng quan

| Thành phần | Công nghệ |
|------------|-----------|
| **Nguồn dữ liệu** | CSV, Oracle |
| **Processing** | Apache Spark |
| **Table Format** | Apache Iceberg |
| **Object Storage** | MinIO |
| **Query Engine** | Trino |
| **BI Tool** | Apache Superset (SQL Lab) |
| **Orchestration** | Docker Compose |

## 🏛️ Kiến trúc

```
CSV / Oracle  →  Spark ETL  →  Iceberg (Bronze/Silver/Gold)  →  Trino  →  Superset
                                        ↓
                                   MinIO (S3)
```

## 📁 Cấu trúc thư mục

```
├── docker/              # Docker Compose & Dockerfiles
├── spark/               # Spark jobs (ETL Bronze/Silver/Gold)
├── trino/               # Trino catalog configs
├── superset/            # Superset configs
├── data/
│   ├── sample/          # Sample data cho test
│   └── raw/             # Raw data (gitignored)
├── scripts/             # Utility scripts
├── tests/               # Data quality tests
├── docs/                # Tài liệu project
└── runbook/             # Hướng dẫn vận hành
```

## 🚀 Quick Start

```bash
# Clone repo
git clone https://github.com/ngntrongnam24/lakehouse-on-premise.git
cd lakehouse-on-premise

# Khởi động services
docker compose up -d

# Kiểm tra trạng thái
docker compose ps
```

## 👥 Team

| Vai trò | Thành viên |
|---------|------------|
| Data Platform | |
| Data Engineer | |
| DataOps | |

## 📌 Quy tắc làm việc

1. Mỗi task có đúng **một owner**
2. Mỗi ngày: cập nhật task board + ít nhất 1 commit
3. Mỗi tuần: demo + review bằng chứng
4. Lỗi > 60 phút: ghi log, nhờ review, **không đổi nhiều config cùng lúc**
5. Tuần cuối: kiểm chứng & bàn giao, **không thêm tính năng mới**

## 🌿 Git Workflow

- `main` — branch ổn định, chỉ merge qua Pull Request
- `dev` — branch phát triển chính
- `feature/<tên>` — branch cho từng task/feature
- `hotfix/<tên>` — sửa lỗi khẩn cấp

```bash
# Tạo feature branch
git checkout dev
git pull origin dev
git checkout -b feature/ten-task

# Khi xong, tạo Pull Request vào dev
```
