# STEP 1 (FAZ A+B) Entegrasyon Notu

Bu paket şunları ekler:
- CBAM portal simulator + producer attestation (Phase A)
- Monitoring plan lifecycle (ETS için) (Phase A)
- Carbon ERP Master Data (Products/Materials/Processes/BOM) + change log (Phase B)
- Process emissions çekirdek fonksiyonları (Phase B)

**Gerekli tek güncelleme:** `src/db/session.py` içinde yeni modeller import edilmelidir.
Bu paket, güncellenmiş `src/db/session.py` dosyasını içerir. Üzerine yazabilirsiniz.

Kurulumdan sonra:
- Streamlit menüsünde `98_CBAM_Portal_Simulator` ve `110_CarbonERP_MasterData` görünür.
