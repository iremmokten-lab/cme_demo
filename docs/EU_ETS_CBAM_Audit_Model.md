# EU ETS + CBAM Audit Model (Denetim Modeli)

Amaç: Company → Verifier → Authority zincirinde **kanıtlanabilir** veri hattı kurmak.

## 1) Denetim paketinin bileşenleri
- Data ingestion kayıtları (dosya, zaman, kullanıcı)
- Veri sözlüğü ve mapping (hangi kolon ne demek)
- Hesaplama snapshot (dataset_hash, factor_set_hash, methodology_hash, result_hash)
- Compliance checks (fail/ok)
- Export edilen raporlar (ETS PDF, CBAM XML/JSON/PDF)
- Signature block (HMAC/Ed25519)
- Master data değişiklik log’u

## 2) Denetim soruları (verifier checklist)
### EU ETS (MRR 2018/2066)
- Activity data kaynakları ve doğruluğu
- Tier logic ve belirsizlik
- QA/QC prosedürleri
- Veri boşlukları ve düzeltici aksiyonlar
- Recalculation / correction trace

### CBAM (2023/956 + uygulama reg.)
- Ürün/CN kod doğruluğu
- Doğrudan / dolaylı emisyon metodolojisi
- actual vs default flag mantığı
- embedded emissions hesap izi
- carbon_price_paid kanıtı
- certificate_required hesap izi
- XML XSD validasyonu

## 3) Örnek “audit trail” akışı
1. Excel yüklenir → DatasetUpload + hash
2. Master data seçilir / güncellenir → change log
3. Snapshot oluşturulur → result_hash
4. Rapor export edilir → audit event
5. Evidence pack hazırlanır → signature

Bu zincir kırılmadığı sürece “regulation-grade” seviyeye yaklaşılır.
