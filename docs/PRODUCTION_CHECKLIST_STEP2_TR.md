# Production Checklist (Adım 2 / 12)

Bu paket, 12 maddenin son 6'sını kod olarak tamamlar:

7) Integration registry (ERP/SCADA bağlantı kayıtları)
8) Async job queue + worker (uzun işler için)
9) Observability (structured logging hook)
10) Cache layer (TTL cache table)
11) Regulation version governance (spec registry + sha)
12) Support bundle export (tek ZIP)

Kabul kriterleri:
- Streamlit'de `180_Integrations_Admin`, `185_Jobs_Queue_Monitor`, `190_Regulation_Specs_Admin`, `195_Support_Bundle` açılır
- DB tabloları oluşur
