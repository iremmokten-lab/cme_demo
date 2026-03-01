# “Watershed seviyesinde” Veri Mimarisi (Kıyas Dokümanı)

Buradaki hedef: ESG dashboard değil, **regülasyon denetimi**.

## 1) Katmanlar
### A) Ingestion layer
- şablon kontrollü yükleme (Excel/CSV/API)
- schema validation
- lineage (dosya → dataset → hesap)

### B) Curated datasets
- normalize edilmiş tablolar
- data quality skorları
- gap detection

### C) Deterministic calculation layer
- factor governance
- methodology lock
- snapshot + replay

### D) Compliance + Export layer
- ETS dataset/PDF
- CBAM XML (XSD doğrulamalı)
- evidence pack

## 2) “Single source of truth”
Bu platformda truth şudur:
- Dataset + Master data + Factor + Methodology + Config
Bunların hepsi snapshot’a bağlanır.

## 3) Operasyonel vs Regülasyonel veri
- Operasyonel tablolar güncel “son durumu” taşır.
- Regülasyonel tablolar versiyonlu ve immutable’dır.
Faz 1, bu ayrımı başlatır.
