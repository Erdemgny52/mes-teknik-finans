from datetime import date

import pandas as pd
import psycopg
import streamlit as st

# -------------------------------------------------
# Mes Teknik Finans Takip Uygulaması
# Hızlandırılmış web sürümü - Supabase PostgreSQL
# -------------------------------------------------

st.set_page_config(
    page_title="Mes Teknik Finans Takip",
    page_icon="💼",
    layout="wide",
)

DB_HOST = st.secrets["DB_HOST"]
DB_NAME = st.secrets["DB_NAME"]
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_PORT = int(st.secrets["DB_PORT"])

DONEMSEL_VERGI_ORANI = 0.20
YILLIK_VERGI_ORANI = 0.25
PAGE_SIZE = 100


# -------------------------------------------------
# Veritabanı bağlantısı
# -------------------------------------------------
@st.cache_resource
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
    with conn.cursor() as cur:
        cur.execute(
            """
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
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cash_movements (
                id BIGSERIAL PRIMARY KEY,
                tarih DATE NOT NULL,
                hesap_tipi VARCHAR(50) NOT NULL,
                islem VARCHAR(50) NOT NULL,
                tutar NUMERIC(12,2) NOT NULL,
                aciklama TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )

        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
        cur.execute("ALTER TABLE cash_movements ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")

        # Performans için indexler
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_tarih ON transactions (tarih)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_islem_turu ON transactions (islem_turu)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_cari_unvan ON transactions (cari_unvan)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_movements_tarih ON cash_movements (tarih)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_cash_movements_created_at ON cash_movements (created_at DESC)")


# -------------------------------------------------
# Yardımcı fonksiyonlar
# -------------------------------------------------
def money(value):
    value = float(value or 0)
    return f"₺{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data(ttl=10)
def read_df(query, params=None):
    conn = get_conn()
    return pd.read_sql(query, conn, params=params)


def execute_query(query, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params or [])


def clear_all_cache():
    st.cache_data.clear()


@st.cache_data(ttl=10)
def get_transactions_filtered(mode, selected_date):
    if mode == "Günlük":
        query = """
            SELECT *
            FROM transactions
            WHERE tarih = %s
            ORDER BY tarih DESC, id DESC
        """
        return read_df(query, [selected_date])

    if mode == "Aylık":
        query = """
            SELECT *
            FROM transactions
            WHERE EXTRACT(MONTH FROM tarih) = %s
              AND EXTRACT(YEAR FROM tarih) = %s
            ORDER BY tarih DESC, id DESC
        """
        return read_df(query, [selected_date.month, selected_date.year])

    query = """
        SELECT *
        FROM transactions
        WHERE EXTRACT(YEAR FROM tarih) = %s
        ORDER BY tarih DESC, id DESC
    """
    return read_df(query, [selected_date.year])


@st.cache_data(ttl=10)
def get_cash_filtered(mode, selected_date):
    if mode == "Günlük":
        query = """
            SELECT *
            FROM cash_movements
            WHERE tarih = %s
            ORDER BY tarih DESC, id DESC
        """
        return read_df(query, [selected_date])

    if mode == "Aylık":
        query = """
            SELECT *
            FROM cash_movements
            WHERE EXTRACT(MONTH FROM tarih) = %s
              AND EXTRACT(YEAR FROM tarih) = %s
            ORDER BY tarih DESC, id DESC
        """
        return read_df(query, [selected_date.month, selected_date.year])

    query = """
        SELECT *
        FROM cash_movements
        WHERE EXTRACT(YEAR FROM tarih) = %s
        ORDER BY tarih DESC, id DESC
    """
    return read_df(query, [selected_date.year])


@st.cache_data(ttl=10)
def get_all_transactions():
    return read_df("SELECT * FROM transactions ORDER BY tarih DESC, id DESC")


@st.cache_data(ttl=10)
def get_transaction_by_id(record_id):
    df = read_df("SELECT * FROM transactions WHERE id = %s LIMIT 1", [int(record_id)])
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


def reset_edit_state():
    if "edit_record_id" in st.session_state:
        del st.session_state["edit_record_id"]


# -------------------------------------------------
# Başlangıç
# -------------------------------------------------
init_db()

st.title("Mes Teknik Finans Takip")
st.caption("Profesyonel, hızlı ve ortak kullanıma uygun sürüm")

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

transactions_df = get_transactions_filtered(rapor_tipi, secili_tarih)
summary = hesap_ozet(transactions_df)

cash_df = get_cash_filtered(rapor_tipi, secili_tarih)

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
        st.dataframe(transactions_df.head(PAGE_SIZE), use_container_width=True, hide_index=True)
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
        kaydet = st.form_submit_button("Kaydı Ekle", use_container_width=True)

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
                clear_all_cache()
                st.success("Kayıt eklendi.")
                st.rerun()

# -------------------------------------------------
# Cari Hesaplar
# -------------------------------------------------
elif menu == "Cari Hesaplar":
    cari_df = transactions_df[transactions_df["cari_unvan"].fillna("") != ""] if not transactions_df.empty else pd.DataFrame()

    if not cari_df.empty:
        st.dataframe(cari_df.head(PAGE_SIZE), use_container_width=True, hide_index=True)

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

        ekle = st.form_submit_button("Hareketi Kaydet", use_container_width=True)

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
                clear_all_cache()
                st.success("Kasa / banka hareketi kaydedildi.")
                st.rerun()

    if not cash_df.empty:
        st.dataframe(cash_df.head(PAGE_SIZE), use_container_width=True, hide_index=True)

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
    tum_df = get_all_transactions()
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
        st.subheader("Kayıt Listesi")
        st.dataframe(transactions_df.head(PAGE_SIZE), use_container_width=True, hide_index=True)

        secenek_df = transactions_df[["id", "tarih", "islem_turu", "kategori", "aciklama", "tutar"]].copy()
        secenek_df["etiket"] = secenek_df.apply(
            lambda r: f"#{int(r['id'])} | {str(r['tarih'])[:10]} | {r['islem_turu']} | {r['kategori']} | {r['aciklama']} | {money(r['tutar'])}",
            axis=1,
        )
        secenekler = dict(zip(secenek_df["etiket"], secenek_df["id"]))

        secili_etiket = st.selectbox("Düzenlenecek / silinecek kayıt", list(secenekler.keys()))
        secili_id = int(secenekler[secili_etiket])

        col_btn1, col_btn2 = st.columns(2)
        if col_btn1.button("Kayıt Düzenle", use_container_width=True):
            st.session_state["edit_record_id"] = secili_id
        if col_btn2.button("Kayıt Sil", type="primary", use_container_width=True):
            execute_query("DELETE FROM transactions WHERE id = %s", [secili_id])
            clear_all_cache()
            reset_edit_state()
            st.success("Kayıt silindi.")
            st.rerun()

        if st.session_state.get("edit_record_id"):
            edit_df = get_transaction_by_id(st.session_state["edit_record_id"])
            if not edit_df.empty:
                row = edit_df.iloc[0]
                st.markdown("---")
                st.subheader(f"Kayıt Düzenleme - #{int(row['id'])}")

                with st.form("edit_form"):
                    e1, e2, e3 = st.columns(3)

                    with e1:
                        edit_tarih = st.date_input("Tarih", value=pd.to_datetime(row["tarih"]).date(), key="edit_tarih")
                        edit_islem_turu = st.selectbox("İşlem Türü", ["Gelir", "Gider"], index=0 if row["islem_turu"] == "Gelir" else 1)
                        kategori_ops = ["Satış", "Servis", "Malzeme", "Personel", "Kira", "Elektrik", "Yakıt", "Vergi", "Diğer"]
                        edit_kategori = st.selectbox(
                            "Kategori",
                            kategori_ops,
                            index=kategori_ops.index(row["kategori"]) if row["kategori"] in kategori_ops else 0,
                        )

                    with e2:
                        edit_alt_kategori = st.text_input("Alt Kategori", value=row["alt_kategori"] or "")
                        odeme_ops = ["Nakit", "Banka", "Kredi Kartı", "Havale/EFT"]
                        edit_odeme_turu = st.selectbox(
                            "Ödeme Türü",
                            odeme_ops,
                            index=odeme_ops.index(row["odeme_turu"]) if row["odeme_turu"] in odeme_ops else 0,
                        )
                        odeme_durumu_ops = ["Ödendi", "Bekliyor", "Kısmi Ödeme"]
                        edit_odeme_durumu = st.selectbox(
                            "Ödeme Durumu",
                            odeme_durumu_ops,
                            index=odeme_durumu_ops.index(row["odeme_durumu"]) if row["odeme_durumu"] in odeme_durumu_ops else 0,
                        )
                        edit_tutar = st.number_input("Tutar (₺)", min_value=0.0, value=float(row["tutar"]), step=100.0)

                    with e3:
                        edit_cari_unvan = st.text_input("Cari Ünvan", value=row["cari_unvan"] or "")
                        edit_personel_adi = st.text_input("Personel Adı", value=row["personel_adi"] or "")
                        gider_ops = ["Servis", "Ofis", "Araç", "Depo", "Personel", "Satış", "Genel"]
                        edit_gider_merkezi = st.selectbox(
                            "Gider Merkezi",
                            gider_ops,
                            index=gider_ops.index(row["gider_merkezi"]) if row["gider_merkezi"] in gider_ops else len(gider_ops) - 1,
                        )
                        edit_aciklama = st.text_input("Açıklama", value=row["aciklama"] or "")

                    edit_notlar = st.text_area("Notlar", value=row["notlar"] or "")

                    eb1, eb2 = st.columns(2)
                    guncelle = eb1.form_submit_button("Değişiklikleri Kaydet", use_container_width=True)
                    iptal = eb2.form_submit_button("İptal", use_container_width=True)

                    if guncelle:
                        if not edit_aciklama.strip() or edit_tutar <= 0:
                            st.error("Lütfen açıklama ve geçerli bir tutar girin.")
                        else:
                            execute_query(
                                """
                                UPDATE transactions
                                SET tarih = %s,
                                    islem_turu = %s,
                                    kategori = %s,
                                    alt_kategori = %s,
                                    aciklama = %s,
                                    tutar = %s,
                                    odeme_turu = %s,
                                    cari_unvan = %s,
                                    personel_adi = %s,
                                    odeme_durumu = %s,
                                    gider_merkezi = %s,
                                    notlar = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                [
                                    edit_tarih,
                                    edit_islem_turu,
                                    edit_kategori,
                                    edit_alt_kategori,
                                    edit_aciklama,
                                    edit_tutar,
                                    edit_odeme_turu,
                                    edit_cari_unvan,
                                    edit_personel_adi,
                                    edit_odeme_durumu,
                                    edit_gider_merkezi,
                                    edit_notlar,
                                    int(row["id"]),
                                ],
                            )
                            clear_all_cache()
                            reset_edit_state()
                            st.success("Kayıt güncellendi.")
                            st.rerun()

                    if iptal:
                        reset_edit_state()
                        st.rerun()
    else:
        st.info("Seçilen dönemde kayıt yok.")

st.markdown("---")
st.caption(
    "Bu sürümde hız için cache, SQL filtreleme ve index eklendi. "
    "Kayıt düzenleme ve silme profesyonel akışla çalışır. Vergi hesabı: önce %20, sonra kalan tutarın %25'i."
)
