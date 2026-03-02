# Production Checklist (Adım 1 / 12)

Bu paket, 12 maddenin ilk 6'sını kod olarak tamamlar:

1) CBAM portal submission workflow (hazırla/kontrol/gönder/durum izle)
2) Portal sandbox connector (config ile)
3) Producer/Supplier attestation zinciri (mevcut sayfaya entegre edilebilir)
4) Verifier workspace persistence (sampling, findings, corrective actions)
5) Dataset approval workflow (governance)
6) Enterprise auth + access audit log (SSO header mode + log format)

Kabul kriterleri:
- Streamlit'de `165_CBAM_Portal_Submission_Workflow`, `160_Data_Governance_Onay`, `170_Dogrulayici_Workspace_Pro` sayfaları açılır
- DB'de tablolar oluşur (create_all ile)
