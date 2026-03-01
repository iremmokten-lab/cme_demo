# 100k Facility ölçeğinde performans tasarımı

Bu doküman “büyüdüğümüzde ne olur?” sorusunu cevaplar.

## 1) DB yaklaşımı
SQLite demo için yeterli.
Prod için:
- PostgreSQL
- RLS (Row Level Security) / tenant isolation
- Partitioning (company_id bazlı)
- Doğru index’ler (company_id, logical_id, is_active, valid_from)

## 2) Read path optimizasyonu
- “aktif kayıtlar” için materialized view / cache
- snapshot oluştururken gerekli subset query
- pagination (UI/ API)

## 3) Write path
- Update yok, insert var → write amplification artar
- ama audit güvenliği için gerekli
- batch insert (import) için transaction + chunk

## 4) Graph (BOM) ölçeği
- cycle check: O(E) + DFS
- çok büyük BOM’da:
  - incremental cycle detection
  - adjacency list cache
  - edge partitioning by root product

## 5) Dosya/rapor üretimi
- export işleri job queue (zaten repo’da jobs yapısı var)
- async worker (ileride)

## 6) Güvenlik
- tenant isolation “hard requirement”
- admin actions ayrı log
- signature keys secret management (Streamlit secrets / vault)
