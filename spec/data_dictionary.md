# Veri Sözlüğü (Data Dictionary)
**Sürüm:** 2026.03.01
Bu doküman, regülasyon-grade çalışmak için gereken veri alanlarını tanımlar.
## Dataset'ler
### energy (`energy.csv`)
- Amaç: Yakıt ve elektrik tüketimi (ETS activity data + CBAM indirect).
- Gerekli: ETS, CBAM, TR_ETS

| Alan | Tip | Zorunlu | Açıklama |
|---|---|---:|---|
| month | YYYY-MM | Evet | Tüketim ayı. |
| facility_id | int | Evet | Tesis kimliği. |
| fuel_type | str | Evet | Yakıt/enerji türü (natural_gas, diesel, electricity vb.). |
| scope | int | Evet | 1=direct combustion, 2=elektrik (indirect). |
| activity_amount | float | Evet | Miktar (birimle birlikte). |
| unit | str | Evet | Nm3, t, MWh vb. |
| emission_factor_kgco2_per_unit | float | Hayır | Eğer ölçüm bazlı EF kullanılıyorsa. Aksi halde factor registry ile eşlenir. |

> Not: Mevcut template dosyaları eski olabilir; bu sözlük regülasyon-grade şemadır.

### production (`production.csv`)
- Amaç: Ürün üretimi ve CBAM ürün miktarları.
- Gerekli: CBAM, TR_ETS

| Alan | Tip | Zorunlu | Açıklama |
|---|---|---:|---|
| month | YYYY-MM | Hayır | Aylık raporlama yapılacaksa. |
| sku | str | Evet | İç ürün kodu. |
| product_name | str | Hayır | Ürün adı. |
| cn_code | str | Hayır | CBAM Annex I CN kodu. |
| sector | str | Hayır | CBAM 6 sektör etiketi. |
| quantity | float | Evet | Toplam üretim/ithalat miktarı (ton veya MWh). |
| quantity_unit | str | Hayır | ton/MWh/kg vb. |
| export_to_eu_quantity | float | Hayır | AB'ye giden miktar. |
| cbam_covered | int(0/1) | Hayır | CBAM kapsam işareti. |
| actual_default_flag | ACTUAL/DEFAULT | Hayır | CBAM veri türü. |

### cbam_products (`cbam_products.csv`)
- Amaç: CBAM ürün bazında rapor satırları (CN code, direct/indirect/embedded).
- Gerekli: CBAM

| Alan | Tip | Zorunlu | Açıklama |
|---|---|---:|---|
| product_id | str | Evet | Ürün kimliği. |
| product_name | str | Evet | Ürün adı. |
| cn_code | str | Evet | CN code. |
| sector | str | Evet | cement/iron_steel/aluminium/fertilisers/electricity/hydrogen. |
| quantity | float | Evet | Miktar. |
| quantity_unit | str | Evet | ton/MWh/kg. |
| direct_emissions_tco2e | float | Evet | Direct emissions. |
| indirect_emissions_tco2e | float | Evet | Indirect emissions. |
| embedded_emissions_tco2e | float | Evet | Direct + Indirect. |
| intensity_tco2e_per_unit | float | Evet | Yoğunluk. |
| actual_default_flag | str | Evet | ACTUAL/DEFAULT. |
| default_value_source | str | Hayır | DEFAULT ise zorunlu. |
| default_value_version | str | Hayır | DEFAULT ise zorunlu. |

> Not: Bu dataset Adım-3'te hesap motoru ile otomatik üretilebilir; şimdilik manuel upload opsiyonu.

## Config Anahtarları

| Anahtar | Gerekli | Açıklama |
|---|---|---|
| period.year | ETS, CBAM, TR_ETS | Rapor yılı. |
| period.quarter | CBAM | CBAM transitional dönemde çeyrek. |
| importer.id | CBAM | Declarant/importer kimliği. |
| monitoring_plan.boundaries | ETS, TR_ETS | İzleme sınırları. |
