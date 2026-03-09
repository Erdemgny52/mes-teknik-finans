from datetime import date

import pandas as pd
import psycopg
import streamlit as st

# -------------------------------------------------
# Mes Teknik Finans Takip Uygulaması
# Web tabanlı - Supabase PostgreSQL bağlantılı
# -------------------------------------------------

st.set_page_config(
    page_title="Mes Teknik Finans Takip",
    page_icon="💼",
    layout="wide",
)

# Supabase bağlantı bilgileri (.streamlit/secrets.toml içinden okunur)
DB_HOST = st.secrets["DB_HOST"]
DB_NAME = st.secrets["DB_NAME"]
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_PORT = int(st.secrets["DB_PORT"])

DONEMSEL_VERGI_ORANI = 0.20
YILLIK_VERGI_ORANI = 0.25


# -------------------------------------------------
# Veritabanı bağlantısı
# -------------------------------------------------
def get_conn():
    return psycopg.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        autocommit=True,
    )


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id BIGSERIAL PRIMARY KEY,
            tarih DATE NOT NULL,
            islem_turu VARCHAR(20) NOT NULL,
            kategori VARCHAR(100) NOT NULL,
            alt_kategori VARCHAR(100),
            aciklama TEXT NOT NULL,
            tutar NUMERIC(12,2) NOT NULL,
            odeme_turu VARCHAR(50),
            cari_unvan VARCHAR(200),
            personel_adi VARCHAR(200),
            odeme_durumu VARCHAR(50) DEFAULT 'Ödendi',
            gider_merkezi VARCHAR(100),
            notlar TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cash_movements (
            id BIGSERIAL PRIMARY KEY,
            tarih DATE NOT NULL,
            hesap_tipi VARCHAR(50) NOT NULL,
            islem VARCHAR(50) NOT NULL,
            tutar NUMERIC(12,2) NOT NULL,
            aciklama TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.close()
    conn.close()


# -------------------------------------------------
# Yardımcı fonksiyonlar
# -------------------------------------------------
def money(value):
    value = float(value or 0)
    return f"₺{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def read_df(query, params=None):
    conn = get_conn()
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


def execute_query(query, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params or [])
    cur.close()
    conn.close()


def filter_period(df, mode, selected_date):
    if df.empty:
        return df

    df = df.copy()
    df["tarih"] = pd.to_datetime(df["tarih"])
    selected_date = pd.to_datetime(selected_date)

    if mode == "Günlük":
        return df[df["tarih"].dt.date == selected_date.date()]
    if mode == "Aylık":
        return df[
            (df["tarih"].dt.month == selected_date.month)
            & (df["tarih"].dt.year == selected_date.year)
        ]
    if mode == "Yıllık":
        return df[df["tarih"].dt.year == selected_date.year]
    return df


def hesap_ozet(df):
    if df.empty:
        return {
            "gelir": 0.0,
            "gider": 0.0,
            "ciro": 0.0,
            "brut_kar": 0.0,
            "donemsel_vergi": 0.0,
            "vergi_sonrasi_kalan": 0.0,
            "yillik_vergi": 0.0,
            "toplam_vergi": 0.0,
            "net_kar": 0.0,
            "tahsil_edilen": 0.0,
            "bekleyen_tahsilat": 0.0,
        }

    gelir = df[df["islem_turu"] == "Gelir"]["tutar"].sum()
    gider = df[df["islem_turu"] == "Gider"]["tutar"].sum()
    ciro = gelir
    brut_kar = gelir - gider

    if brut_kar > 0:
        donemsel_vergi = brut_kar * DONEMSEL_VERGI_ORANI
        vergi_sonrasi_kalan = brut_kar - donemsel_vergi
        yillik_vergi = vergi_sonrasi_kalan * YILLIK_VERGI_ORANI
        toplam_vergi = donemsel_vergi + yillik_vergi
        net_kar = brut_kar - toplam_vergi
    else:
        donemsel_vergi = 0.0
        vergi_sonrasi_kalan = brut_kar
        yillik_vergi = 0.0
        toplam_vergi = 0.0
        net_kar = brut_kar

    tahsil_edilen = df[
        (df["islem_turu"] == "Gelir")
        & (df["odeme_durumu"].fillna("Ödendi") == "Ödendi")
    ]["tutar"].sum()

    bekleyen_tahsilat = df[
        (df["islem_turu"] == "Gelir")
        & (df["odeme_durumu"].fillna("Ödendi") != "Ödendi")
    ]["tutar"].sum()

    return {
        "gelir": float(gelir),
        "gider": float(gider),
        "ciro": float(ciro),
        "brut_kar": float(brut_kar),
        "donemsel_vergi": float(donemsel_vergi),
        "vergi_sonrasi_kalan": float(vergi_sonrasi_kalan),
        "yillik_vergi": float(yillik_vergi),
        "toplam_vergi": float(toplam_vergi),
        "net_kar": float(net_kar),
        "tahsil_edilen": float(tahsil_edilen),
        "bekleyen_tahsilat": float(bekleyen_tahsilat),
    }


# -------------------------------------------------
# Başlangıç
# -------------------------------------------------
init_db()

st.title("Mes Teknik Finans Takip")
st.caption("Web tabanlı ortak kullanım sürümü - iPad, Android ve bilgisayar uyumlu")

st.sidebar.header("Filtreler")
rapor_tipi = st.sidebar.selectbox("Rapor Türü", ["Günlük", "Aylık", "Yıllık"])
secili_tarih = st.sidebar.date_input("Tarih Seç", value=date.today())
menu = st.sidebar.radio(
    "Bölüm",
    [
        "Dashboard",
        "Yeni Kayıt",
        "Cari Hesaplar",
        "Kasa / Banka",
        "Borç / Alacak Raporu",
        "Tüm Hareketler",
    ],
)

transactions_df = read_df("SELECT * FROM transactions ORDER BY tarih DESC, id DESC")
transactions_df = filter_period(transactions_df, rapor_tipi, secili_tarih)
summary = hesap_ozet(transactions_df)

cash_df = read_df("SELECT * FROM cash_movements ORDER BY tarih DESC, id DESC")
cash_df = filter_period(cash_df, rapor_tipi, secili_tarih)

# -------------------------------------------------
# Dashboard
# -------------------------------------------------
if menu == "Dashboard":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Toplam Gelir", money(summary["gelir"]))
    c2.metric("Toplam Gider", money(summary["gider"]))
    c3.metric("Ciro", money(summary["ciro"]))
    c4.metric("Net Kâr", money(summary["net_kar"]))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Brüt Kâr", money(summary["brut_kar"]))
    c6.metric("%20 İlk Vergi", money(summary["donemsel_vergi"]))
    c7.metric("İlk Vergiden Sonra Kalan", money(summary["vergi_sonrasi_kalan"]))
    c8.metric("%25 Kalan Tutar Vergisi", money(summary["yillik_vergi"]))

    c9, c10, c11 = st.columns(3)
    c9.metric("Toplam Vergi", money(summary["toplam_vergi"]))
    c10.metric("Tahsil Edilen Gelir", money(summary["tahsil_edilen"]))
    c11.metric("Bekleyen Tahsilat", money(summary["bekleyen_tahsilat"]))

    if not transactions_df.empty:
        grafik_df = transactions_df.groupby(["tarih", "islem_turu"], as_index=False)["tutar"].sum()
        st.line_chart(grafik_df, x="tarih", y="tutar", color="islem_turu")
        st.dataframe(transactions_df, use_container_width=True, hide_index=True)
    else:
        st.info("Seçilen dönem için kayıt bulunamadı.")

# -------------------------------------------------
# Yeni Kayıt
# -------------------------------------------------
elif menu == "Yeni Kayıt":
    with st.form("kayit_formu", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            tarih = st.date_input("Tarih", value=date.today())
            islem_turu = st.selectbox("İşlem Türü", ["Gelir", "Gider"])
            kategori = st.selectbox(
                "Kategori",
                ["Satış", "Servis", "Malzeme", "Personel", "Kira", "Elektrik", "Yakıt", "Vergi", "Diğer"],
            )

        with c2:
            alt_kategori = st.text_input("Alt Kategori")
            odeme_turu = st.selectbox("Ödeme Türü", ["Nakit", "Banka", "Kredi Kartı", "Havale/EFT"])
            odeme_durumu = st.selectbox("Ödeme Durumu", ["Ödendi", "Bekliyor", "Kısmi Ödeme"])
            tutar = st.number_input("Tutar (₺)", min_value=0.0, step=100.0)

        with c3:
            cari_unvan = st.text_input("Cari Ünvan")
            personel_adi = st.text_input("Personel Adı")
            gider_merkezi = st.selectbox("Gider Merkezi", ["Servis", "Ofis", "Araç", "Depo", "Personel", "Satış", "Genel"])
            aciklama = st.text_input("Açıklama")

        notlar = st.text_area("Notlar")
        kaydet = st.form_submit_button("Kaydı Ekle")

        if kaydet:
            if not aciklama.strip() or tutar <= 0:
                st.error("Lütfen açıklama ve geçerli bir tutar girin.")
            else:
                execute_query(
                    """
                    INSERT INTO transactions (
                        tarih, islem_turu, kategori, alt_kategori, aciklama, tutar,
                        odeme_turu, cari_unvan, personel_adi, odeme_durumu, gider_merkezi, notlar
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        tarih,
                        islem_turu,
                        kategori,
                        alt_kategori,
                        aciklama,
                        tutar,
                        odeme_turu,
                        cari_unvan,
                        personel_adi,
                        odeme_durumu,
                        gider_merkezi,
                        notlar,
                    ],
                )
                st.success("Kayıt eklendi.")
                st.rerun()

# -------------------------------------------------
# Cari Hesaplar
# -------------------------------------------------
elif menu == "Cari Hesaplar":
    cari_df = transactions_df[transactions_df["cari_unvan"].fillna("") != ""] if not transactions_df.empty else pd.DataFrame()

    if not cari_df.empty:
        st.dataframe(cari_df, use_container_width=True, hide_index=True)

        cari_ozet = (
            cari_df.groupby("cari_unvan")
            .apply(
                lambda g: pd.Series(
                    {
                        "Toplam Gelir": g[g["islem_turu"] == "Gelir"]["tutar"].sum(),
                        "Toplam Gider": g[g["islem_turu"] == "Gider"]["tutar"].sum(),
                        "Bekleyen Tahsilat": g[
                            (g["islem_turu"] == "Gelir")
                            & (g["odeme_durumu"].fillna("Ödendi") != "Ödendi")
                        ]["tutar"].sum(),
                        "Cari Bakiye": g[g["islem_turu"] == "Gelir"]["tutar"].sum()
                        - g[g["islem_turu"] == "Gider"]["tutar"].sum(),
                    }
                )
            )
            .reset_index()
        )

        st.dataframe(cari_ozet, use_container_width=True, hide_index=True)
    else:
        st.info("Cari kayıt bulunamadı.")

# -------------------------------------------------
# Kasa / Banka
# -------------------------------------------------
elif menu == "Kasa / Banka":
    with st.form("cash_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            tarih = st.date_input("Tarih", value=date.today(), key="cash_date")
            hesap_tipi = st.selectbox("Hesap Tipi", ["Kasa", "Banka"])

        with c2:
            islem = st.selectbox("İşlem", ["Para Girişi", "Para Çıkışı", "Virman"])
            tutar = st.number_input("Tutar", min_value=0.0, step=100.0, key="cash_amount")

        with c3:
            aciklama = st.text_input("Açıklama", key="cash_desc")

        ekle = st.form_submit_button("Hareketi Kaydet")

        if ekle:
            if tutar <= 0:
                st.error("Geçerli bir tutar girin.")
            else:
                execute_query(
                    """
                    INSERT INTO cash_movements (tarih, hesap_tipi, islem, tutar, aciklama)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [tarih, hesap_tipi, islem, tutar, aciklama],
                )
                st.success("Kasa / banka hareketi kaydedildi.")
                st.rerun()

    if not cash_df.empty:
        st.dataframe(cash_df, use_container_width=True, hide_index=True)

        kasa_giris = cash_df[
            (cash_df["hesap_tipi"] == "Kasa") & (cash_df["islem"] == "Para Girişi")
        ]["tutar"].sum()
        kasa_cikis = cash_df[
            (cash_df["hesap_tipi"] == "Kasa") & (cash_df["islem"] == "Para Çıkışı")
        ]["tutar"].sum()
        banka_giris = cash_df[
            (cash_df["hesap_tipi"] == "Banka") & (cash_df["islem"] == "Para Girişi")
        ]["tutar"].sum()
        banka_cikis = cash_df[
            (cash_df["hesap_tipi"] == "Banka") & (cash_df["islem"] == "Para Çıkışı")
        ]["tutar"].sum()

        kasa_bakiye = kasa_giris - kasa_cikis
        banka_bakiye = banka_giris - banka_cikis

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Kasa Giriş", money(kasa_giris))
        k2.metric("Kasa Çıkış", money(kasa_cikis))
        k3.metric("Kasa Bakiye", money(kasa_bakiye))
        k4.metric("Banka Bakiye", money(banka_bakiye))
    else:
        st.info("Kasa / banka hareketi bulunamadı.")

# -------------------------------------------------
# Borç / Alacak Raporu
# -------------------------------------------------
elif menu == "Borç / Alacak Raporu":
    tum_df = read_df("SELECT * FROM transactions ORDER BY tarih DESC, id DESC")
    cari_df = tum_df[tum_df["cari_unvan"].fillna("") != ""] if not tum_df.empty else pd.DataFrame()

    if not cari_df.empty:
        rapor = (
            cari_df.groupby("cari_unvan")
            .apply(
                lambda g: pd.Series(
                    {
                        "Toplam Gelir": g[g["islem_turu"] == "Gelir"]["tutar"].sum(),
                        "Toplam Gider": g[g["islem_turu"] == "Gider"]["tutar"].sum(),
                        "Bekleyen Tahsilat": g[
                            (g["islem_turu"] == "Gelir")
                            & (g["odeme_durumu"].fillna("Ödendi") != "Ödendi")
                        ]["tutar"].sum(),
                        "Net Bakiye": g[g["islem_turu"] == "Gelir"]["tutar"].sum()
                        - g[g["islem_turu"] == "Gider"]["tutar"].sum(),
                    }
                )
            )
            .reset_index()
            .sort_values("Net Bakiye", ascending=False)
        )
        st.dataframe(rapor, use_container_width=True, hide_index=True)
    else:
        st.info("Cari bazlı veri yok.")

# -------------------------------------------------
# Tüm Hareketler
# -------------------------------------------------
elif menu == "Tüm Hareketler":
    if not transactions_df.empty:
        st.dataframe(transactions_df, use_container_width=True, hide_index=True)

        secili_id = st.selectbox("Silinecek kayıt ID", transactions_df["id"].tolist())

        if st.button("Seçili Kaydı Sil"):
            execute_query("DELETE FROM transactions WHERE id = %s", [int(secili_id)])
            st.success("Kayıt silindi.")
            st.rerun()
    else:
        st.info("Seçilen dönemde kayıt yok.")

st.markdown("---")
st.caption(
    "Vergi hesabı şu mantıkla yapılır: önce brüt kârdan %20 vergi düşülür, sonra kalan tutarın %25'i alınır. "
    "Bu panel işletme içi takip içindir; resmî muhasebe için mali müşavir kontrolü gerekir."
)