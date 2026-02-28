# AUDITABILITY (Denetlenebilirlik)

Bu proje, aynı input + aynı config + aynı factor set + aynı metodoloji ile aynı sonucu üretmek üzere tasarlanmıştır.

## Hash Mantığı

- input_hash: InputBundle'ın canonical JSON SHA-256 hash'i
- result_hash: ResultBundle'ın canonical JSON SHA-256 hash'i

Canonical JSON:
- sort_keys=True
- separators=(',', ':')
- ensure_ascii=False

## Snapshot (Immutable)

Kilitlenen snapshot:
- değiştirilemez
- silinemez

## Replay

src/mrv/replay.py içindeki replay(snapshot_id):
- dataset URI'ları ile yeniden hesaplar
- input_hash_match/result_hash_match üretir

## Evidence Pack

Evidence pack ZIP:
- manifest.json (tüm dosyaların SHA-256 hash'i)
- signature.json (HMAC-SHA256, anahtar varsa)
- CBAM/ETS/TR-ETS JSON + CBAM XML
- PDF raporlar (ETS/CBAM/Compliance)
- verification_case.json
- evidence dosyaları ve index
