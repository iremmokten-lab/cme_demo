# Faz 2 — Karbon Maliyeti Motoru (Sade Anlatım)

Bu faz, snapshot sonuçlarını **maliyete** çevirir.

## Ne yapar?
1) ETS maliyeti (€/TL)
- Scope 1 emisyon
- ücretsiz tahsis
- banked/devreden
- EUA fiyatı
- kur

2) CBAM maliyeti (€/TL)
- embedded emissions
- payable share (yıla göre artar; 2026+)
- ödenmiş karbon fiyatı varsa düşer
- sertifika gereksinimi ve tahmini ödeme

3) Rapor üretir
- storage/reports/<snapshot_id>/carbon_cost.json
- storage/reports/<snapshot_id>/carbon_cost.pdf

Ayrıca DB'de `reports` tablosuna kaydeder (audit zinciri için).

## Otomatik bağlama
Snapshot üretildiğinde sistem otomatik olarak carbon cost raporlarını da üretir.
(Consultant Panel'de tekrar buton basmaya gerek kalmaz.)

## Senaryo karşılaştırma
UI ekranında Snapshot A ve Snapshot B seçip farkları (B - A) görebilirsiniz.
