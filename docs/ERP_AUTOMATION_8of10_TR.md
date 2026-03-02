# ERP Otomasyonu 8/10 (Tek Paket)

Bu paket şunları sağlar:

- Connection Registry (SAP/Logo/Netsis/SCADA/Generic) — tek tablo + UI
- Mapping Engine (external field → internal canonical field) — versiyonlu + onaylı
- Ingestion Pipeline:
  - Connector ile veri çek
  - Normalize et (canonical schema)
  - DatasetUpload kaydı oluştur (mevcut motorun kullanacağı formda)
  - DLQ (dead-letter) ile hatalı satırları ayır
- Job Queue + Worker:
  - Uzun işlerde kuyruğa al
  - Streamlit içinde manuel worker butonu
  - İstersen `python scripts/run_erp_worker.py` ile worker loop

Kurulum:
1) ZIP içeriğini proje köküne kopyala (aynı yollar)
2) `requirements.txt` içindeki requests satırını ekle
3) `app.py` veya mevcut başlangıçta `init_db()` zaten çağrılıyorsa yeterli
4) Streamlit menüsünde `200_ERP_Otomasyon_Merkezi` sayfasını aç

Not:
- Streamlit Cloud gerçek background worker çalıştırmaz; manuel tetik veya ayrı process gerekir.
