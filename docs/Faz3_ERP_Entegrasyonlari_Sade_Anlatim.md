# Faz 3 — ERP Entegrasyonları (Sade Anlatım)

Bu fazın amacı: **ERP'deki veriyi** platformun anlayacağı “dataset” formatına çevirip, hesap motoruna güvenli şekilde sokmaktır.

## 1) Nasıl çalışır?

1. **Bağlantı tanımlarsın** (SAP / LOGO / NETSIS / Custom)
2. ERP'den veri alırsın:
   - ya **CSV/JSON export** alıp yüklersin
   - ya da **REST/OData** ile çekersin
3. Sistem bunu otomatik olarak:
   - CSV'ye dönüştürür
   - storage'a kaydeder
   - **DatasetUpload** kaydı açar (audit izi)

Sonuç: Snapshot üretirken artık ERP kaynaklı datasetleri kullanabilirsin.

## 2) Neden “DatasetUpload” önemli?

Çünkü platformun denetim zinciri şudur:

Company → Carbon Platform → Compliance Report → Verifier → Authority

Bu zincirde her veri kaynağı **kanıtlanabilir** olmalı. DatasetUpload:
- dosyanın hash'ini
- kaydedildiği zamanı
- kim yüklediğini
- veri kalitesi skorunu
sakar.

## 3) Güvenlik: Secret nasıl yönetilir?

Bu fazda token/şifre **DB'ye yazılmaz**.

Sen bağlantıda sadece `secret_ref` yazarsın.
Gerçek secret Streamlit Cloud secrets/env'den okunur:

`ERP_SECRET_<secret_ref>=...`

Örnek:
- secret_ref: `DEMO1`
- env: `ERP_SECRET_DEMO1=MYTOKEN`

## 4) Mapping nedir?

ERP kolon isimleri platformdakinden farklıysa eşleştirme yaparsın.

Örnek:
- ERP: `qty`
- Platform: `fuel_quantity`

Mapping JSON:
`{"qty": "fuel_quantity", "unit": "fuel_unit"}`

Transform (opsiyonel):
- boş birimleri doldur
- sayısal kolonları çarp

## 5) Hangi dataset'ler hedeflenir?

- energy
- production
- materials
- cbam_products
- bom_precursors

Bu, Faz 0 Excel ingestion ile aynı dataset tipleridir. Yani ERP entegrasyonu “Excel yerine ERP” demektir.
