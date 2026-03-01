# Faz 4 — AI Optimizasyon & Senaryo Motoru (Sade Anlatım)

Bu fazın amacı: **emisyonu azaltma aksiyonlarını** düzenli ve denetlenebilir şekilde önerip,
bunların **maliyet etkisini** (ETS + CBAM tahmini) senaryo olarak göstermektir.

## 1) Ne üretir?
- Hotspot listesi (en büyük sürücüler)
- Önerilen aksiyonlar (rule-based)
- Abatement Cost Curve (MACC) tablosu
- Kısıtlara göre portföy seçimi (deterministik greedy)
- Senaryo simülasyonu:
  - baz emisyon vs senaryo emisyon
  - ETS maliyeti tahmini
  - CBAM sertifika maliyeti tahmini (compute_cbam_liability)

## 2) Neden denetim açısından güvenli?
- Sadece **snapshot sonuçları + config.ai** kullanır.
- Aynı snapshot + aynı config → aynı öneri/sonuç.
- Evidence pack içine “ai_optimization.json / pdf” eklenebilir.

## 3) Nereden kullanılır?
Streamlit menüsünde:
- **AI & Optimizasyon** sayfası

Bu sayfada:
- hotspot + öneriler görülür
- portföy seçimi + senaryo çıktısı görülür
- “AI Raporunu Üret & Kaydet” ile JSON + PDF üretilir

## 4) Not
Bu faz, resmi beyan değildir.
Ama denetçi/uyum süreçlerinde “hangi aksiyonların neden seçildiğini” açıklamak için
**kanıtlanabilir ve tekrar üretilebilir** bir temel sağlar.
