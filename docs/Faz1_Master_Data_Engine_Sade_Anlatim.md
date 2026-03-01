# Faz 1 — Master Data Engine (Sade Anlatım)

Bu bölüm, platformun **“sabit kayıt defteri”** gibi çalışmasını sağlar.

## Neden gerekli?
Karbon raporlarında en büyük risk şudur:

> “Geçen yılın raporu, bugün birileri veriyi değiştirdiği için farklı çıkmasın.”

Bu yüzden Master Data Engine:
- **Ürün, tesis, CN kodu, BOM** gibi temel verileri yönetir.
- Güncelleme yapıldığında **eski kayıt silinmez**.
- Sistem yeni bir **versiyon** oluşturur.

## Ne kazanıyoruz?
- Denetimde “kim, neyi ne zaman değiştirdi?” netleşir.
- Snapshot/replay (sizin Faz 0/engine tarafı) bozulmaz.
- Verifier ve otorite karşısında güven artar.

## İçerik
1. **Tesisler**: Tesis adı, ülke, sektör. Güncelleme = yeni versiyon.
2. **Ürünler**: Ürün adı, CN kodu, sektör. Güncelleme = yeni versiyon.
3. **BOM (Ürün ağacı)**: Ürünlerin girdilerini bağlayan ağ. Sistem döngüye izin vermez.
4. **CN Kodları**: CN kod açıklaması ve sektör bilgisi için global tablo (ileride TARIC ile toplu yükleme).

## Denetim izi (Change Log)
Her değişiklikte sistem:
- önceki kaydın hash’ini
- yeni kaydın hash’ini
saklar.

Bu, “kanıt” üretmek için kullanılır.
