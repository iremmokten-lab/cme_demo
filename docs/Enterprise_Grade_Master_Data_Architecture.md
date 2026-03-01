# Enterprise-grade Master Data Architecture (Regulation-grade)

Bu doküman Faz 1’in “global ürün” seviyesine büyüme planını anlatır.

## 1) Hedef Çıktı
- deterministic
- audit-ready
- verifier-ready
- multi-tenant secure
- replayable

## 2) Master Data kategorileri
### Tenant-level (şirkete özel)
- Facilities
- Products
- BOM / precursor relationships
- Internal product taxonomy (opsiyonel)

### Global (regulator datasetleri)
- CN code dictionary (TARIC/Combined Nomenclature)
- Default emission factors (CBAM default listeleri)
- ETS referans listeleri (NACE/installation types vb.)

## 3) Versioning yaklaşımı
**Asla “update in place” yok.**

- Her değişiklik yeni satır = yeni versiyon
- Eski kayıt “valid_to” ile kapatılır ve pasiflenir
- Snapshot oluşurken ilgili tarihe göre kayıt seçilir:

```
valid_from <= snapshot_date AND (valid_to IS NULL OR valid_to >= snapshot_date)
```

## 4) Değişiklik Kaydı (Change log)
Her kayıt değişiminde:
- old_hash (canonical JSON + SHA256)
- new_hash
- user_id
- timestamp
- reason / note

Bu log, **Evidence Pack** içine de dahil edilebilir.

## 5) API / UI katmanı
- UI: Streamlit panel (danışman rolü)
- Service: business rules
- Repository: DB erişimi

Bu ayrım test edilebilirliği ve regülasyon denetlenebilirliğini artırır.

## 6) Snapshot bağlama (bir sonraki adım)
Master data versiyonlarının snapshot içine “lock” edilmesi gerekir:
- product_version_ids
- facility_version_ids
- bom_edge_version_ids

Böylece replay sırasında “bugünkü master data” ile değil,
snapshot içindeki master data ile hesap yapılır.
