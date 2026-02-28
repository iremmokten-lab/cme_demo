# AUDITABILITY

Bu proje snapshot + replay + evidence pack ile denetlenebilirlik sağlar.

- input_hash/result_hash deterministiktir
- kilitli snapshot değiştirilemez
- evidence pack manifest.json her dosyanın hash'ini taşır
- signature.json HMAC-SHA256 içerir (EVIDENCE_PACK_HMAC_KEY)

