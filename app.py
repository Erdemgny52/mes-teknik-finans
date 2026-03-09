from datetime import date, timedelta

import pandas as pd
import psycopg
import streamlit as st

# -------------------------------------------------
# Mes Teknik Finans Takip Uygulaması
# FAZ 1 - Profesyonel ve hızlandırılmış sürüm
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
PAGE_SIZE_DEFAULT = 100


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
        prepare_threshold=None,
    )

def init_db():
    conn = get_conn()
    with conn.cursor() as cur:
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
                vade_tarihi DATE,
                tahsilat_tarihi DATE,
                tahsilat_notu TEXT,
                notlar TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
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
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Eski tabloya yeni kolonları ekle
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
        cur.execute("ALTER TABLE cash_movements ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS vade_tarihi DATE")
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tahsilat_tarihi DATE")
        cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS tahsilat_notu TEXT")

        # Performans indexleri
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_tarih ON transactions (tarih)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_islem_turu ON transactions (islem_turu)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_cari_unvan ON transactions (cari_unvan)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_odeme_durumu ON transactions (odeme_durumu)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_vade_tarihi ON transactions (vade_tarihi)")
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
    with conn.cursor() as cur:
        cur.execute(query, params or [])
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)

def execute_query(query, params=None):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params or [])


def clear_all_cache():
    st.cache_data.clear()


def reset_edit_state():
    if "edit_record_id" in st.session_state:
        del st.session_state["edit_record_id"]


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
    return read_df("SELECT * FROM transactions WHERE id = %s LIMIT 1", [int(record_id)])


@st.cache_data(ttl=10)
def get_filter_options():
    categories = read_df("SELECT DISTINCT kategori FROM transactions WHERE kategori IS NOT NULL AND kategori <> '' ORDER BY kategori")
    cariler = read_df("SELECT DISTINCT cari_unvan FROM transactions WHERE cari_unvan IS NOT NULL AND cari_unvan <> '' ORDER BY cari_unvan")
    personeller = read_df("SELECT DISTINCT personel_adi FROM transactions WHERE personel_adi IS NOT NULL AND personel_adi <> '' ORDER BY personel_adi")
    odeme_durumlari = read_df("SELECT DISTINCT odeme_durumu FROM transactions WHERE odeme_durumu IS NOT NULL AND odeme_durumu <> '' ORDER BY odeme_durumu")

    return {
        "kategori": categories["kategori"].tolist() if ("kategori" in categories.columns and not categories.empty) else [],
        "cari": cariler["cari_unvan"].tolist() if ("cari_unvan" in cariler.columns and not cariler.empty) else [],
        "personel": personeller["personel_adi"].tolist() if ("personel_adi" in personeller.columns and not personeller.empty) else [],
        "odeme_durumu": odeme_durumlari["odeme_durumu"].tolist() if ("odeme_durumu" in odeme_durumlari.columns and not odeme_durumlari.empty) else [],
    }

@st.cache_data(ttl=10)
def get_transactions_advanced(
    mode,
    selected_date,
    search_text,
    islem_turu,
    kategori,
    cari,
    personel,
    odeme_durumu,
    min_tutar,
    max_tutar,
    only_overdue,
    page,
    page_size,
):
    where_clauses = []
    params = []

    if mode == "Günlük":
        where_clauses.append("tarih = %s")
        params.append(selected_date)
    elif mode == "Aylık":
        where_clauses.append("EXTRACT(MONTH FROM tarih) = %s")
        where_clauses.append("EXTRACT(YEAR FROM tarih) = %s")
        params.extend([selected_date.month, selected_date.year])
    else:
        where_clauses.append("EXTRACT(YEAR FROM tarih) = %s")
        params.append(selected_date.year)

    if search_text:
        where_clauses.append(
            "(COALESCE(aciklama,'') ILIKE %s OR COALESCE(cari_unvan,'') ILIKE %s OR COALESCE(personel_adi,'') ILIKE %s OR COALESCE(kategori,'') ILIKE %s)"
        )
        search_param = f"%{search_text}%"
        params.extend([search_param, search_param, search_param, search_param])

    if islem_turu != "Hepsi":
        where_clauses.append("islem_turu = %s")
        params.append(islem_turu)

    if kategori != "Hepsi":
        where_clauses.append("kategori = %s")
        params.append(kategori)

    if cari != "Hepsi":
        where_clauses.append("cari_unvan = %s")
        params.append(cari)

    if personel != "Hepsi":
        where_clauses.append("personel_adi = %s")
        params.append(personel)

    if odeme_durumu != "Hepsi":
        where_clauses.append("odeme_durumu = %s")
        params.append(odeme_durumu)

    if min_tutar is not None and float(min_tutar) > 0:
        where_clauses.append("tutar >= %s")
        params.append(float(min_tutar))

    if max_tutar is not None and float(max_tutar) > 0:
        where_clauses.append("tutar <= %s")
        params.append(float(max_tutar))

    if only_overdue:
        where_clauses.append("islem_turu = 'Gelir'")
        where_clauses.append("COALESCE(odeme_durumu, 'Ödendi') <> 'Ödendi'")
        where_clauses.append("vade_tarihi IS NOT NULL")
        where_clauses.append("vade_tarihi < CURRENT_DATE")

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    count_query = f"SELECT COUNT(*) AS total_count FROM transactions WHERE {where_sql}"
    count_df = read_df(count_query, params)
    total_count = int(count_df.iloc[0]["total_count"]) if not count_df.empty else 0

    offset = page * page_size
    data_query = f"""
        SELECT *
        FROM transactions
        WHERE {where_sql}
        ORDER BY tarih DESC, id DESC
        LIMIT %s OFFSET %s
    """
    data_params = params + [page_size, offset]
    data_df = read_df(data_query, data_params)

    return data_df, total_count


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


@st.cache_data(ttl=10)
def get_dashboard_stats():
    df = get_all_transactions()
    today = pd.Timestamp(date.today())
    next_7 = today + pd.Timedelta(days=7)
    next_30 = today + pd.Timedelta(days=30)

    if df.empty:
        return {
            "overdue_total": 0.0,
            "due_7_total": 0.0,
            "due_30_total": 0.0,
            "collection_rate": 0.0,
            "top_income_category": "-",
            "top_expense_category": "-",
            "risk_cari": "-",
            "monthly_chart": pd.DataFrame(),
            "expense_chart": pd.DataFrame(),
        }

    df = df.copy()
    df["tarih"] = pd.to_datetime(df["tarih"])
    if "vade_tarihi" in df.columns:
        df["vade_tarihi"] = pd.to_datetime(df["vade_tarihi"], errors="coerce")

    gelir_df = df[df["islem_turu"] == "Gelir"].copy()
    gider_df = df[df["islem_turu"] == "Gider"].copy()

    overdue_df = gelir_df[
        (gelir_df["odeme_durumu"].fillna("Ödendi") != "Ödendi")
        & (gelir_df["vade_tarihi"].notna())
        & (gelir_df["vade_tarihi"] < today)
    ]

    due_7_df = gelir_df[
        (gelir_df["odeme_durumu"].fillna("Ödendi") != "Ödendi")
        & (gelir_df["vade_tarihi"].notna())
        & (gelir_df["vade_tarihi"] >= today)
        & (gelir_df["vade_tarihi"] <= next_7)
    ]

    due_30_df = gelir_df[
        (gelir_df["odeme_durumu"].fillna("Ödendi") != "Ödendi")
        & (gelir_df["vade_tarihi"].notna())
        & (gelir_df["vade_tarihi"] >= today)
        & (gelir_df["vade_tarihi"] <= next_30)
    ]

    tahsil_edilen = gelir_df[gelir_df["odeme_durumu"].fillna("Ödendi") == "Ödendi"]["tutar"].sum()
    toplam_gelir = gelir_df["tutar"].sum()
    collection_rate = (float(tahsil_edilen) / float(toplam_gelir) * 100) if toplam_gelir > 0 else 0.0

    top_income_category = "-"
    if not gelir_df.empty:
        top_income_series = gelir_df.groupby("kategori")["tutar"].sum().sort_values(ascending=False)
        if not top_income_series.empty:
            top_income_category = top_income_series.index[0]

    top_expense_category = "-"
    if not gider_df.empty:
        top_expense_series = gider_df.groupby("kategori")["tutar"].sum().sort_values(ascending=False)
        if not top_expense_series.empty:
            top_expense_category = top_expense_series.index[0]

    risk_cari = "-"
    if not overdue_df.empty:
        risk_series = overdue_df.groupby("cari_unvan")["tutar"].sum().sort_values(ascending=False)
        risk_series = risk_series[risk_series.index.notna()]
        if not risk_series.empty:
            risk_cari = risk_series.index[0]

    df["Yıl-Ay"] = df["tarih"].dt.to_period("M").astype(str)
    monthly_chart = (
        df.pivot_table(
            index="Yıl-Ay",
            columns="islem_turu",
            values="tutar",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )
    if "Gelir" not in monthly_chart.columns:
        monthly_chart["Gelir"] = 0.0
    if "Gider" not in monthly_chart.columns:
        monthly_chart["Gider"] = 0.0

    expense_chart = pd.DataFrame()
    if not gider_df.empty:
        expense_chart = (
            gider_df.groupby("gider_merkezi", dropna=False)["tutar"]
            .sum()
            .reset_index()
            .rename(columns={"gider_merkezi": "Gider Merkezi", "tutar": "Tutar"})
            .sort_values("Tutar", ascending=False)
        )

    return {
        "overdue_total": float(overdue_df["tutar"].sum()) if not overdue_df.empty else 0.0,
        "due_7_total": float(due_7_df["tutar"].sum()) if not due_7_df.empty else 0.0,
        "due_30_total": float(due_30_df["tutar"].sum()) if not due_30_df.empty else 0.0,
        "collection_rate": float(collection_rate),
        "top_income_category": top_income_category,
        "top_expense_category": top_expense_category,
        "risk_cari": risk_cari,
        "monthly_chart": monthly_chart,
        "expense_chart": expense_chart,
    }


# -------------------------------------------------
# Başlangıç
# -------------------------------------------------
init_db()

st.title("Mes Teknik Finans Takip")
st.caption("FAZ 1 tam sürüm - profesyonel, hızlı ve ortak kullanıma uygun")

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
dashboard_stats = get_dashboard_stats()

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

    c9, c10, c11, c12 = st.columns(4)
    c9.metric("Toplam Vergi", money(summary["toplam_vergi"]))
    c10.metric("Tahsil Edilen Gelir", money(summary["tahsil_edilen"]))
    c11.metric("Bekleyen Tahsilat", money(summary["bekleyen_tahsilat"]))
    c12.metric("Tahsilat Oranı", f"%{dashboard_stats['collection_rate']:.1f}")

    st.markdown("---")
    st.subheader("Nakit Akış ve Risk Özeti")

    n1, n2, n3, n4 = st.columns(4)
    n1.metric("Vadesi Geçmiş Tahsilat", money(dashboard_stats["overdue_total"]))
    n2.metric("7 Gün İçinde Tahsilat", money(dashboard_stats["due_7_total"]))
    n3.metric("30 Gün İçinde Tahsilat", money(dashboard_stats["due_30_total"]))
    n4.metric("Tahmini Nakit Girişi", money(dashboard_stats["due_30_total"]))

    r1, r2, r3 = st.columns(3)
    r1.metric("En Çok Gelir Getiren", dashboard_stats["top_income_category"])
    r2.metric("En Yüksek Gider Kategorisi", dashboard_stats["top_expense_category"])
    r3.metric("En Riskli Cari", dashboard_stats["risk_cari"])

    st.markdown("---")
    g1, g2 = st.columns(2)

    with g1:
        st.subheader("Aylık Gelir / Gider")
        monthly_chart = dashboard_stats["monthly_chart"]
        if not monthly_chart.empty:
            st.bar_chart(monthly_chart.set_index("Yıl-Ay")[["Gelir", "Gider"]])
        else:
            st.info("Aylık grafik için veri yok.")

    with g2:
        st.subheader("Gider Merkezi Dağılımı")
        expense_chart = dashboard_stats["expense_chart"]
        if not expense_chart.empty:
            st.bar_chart(expense_chart.set_index("Gider Merkezi")["Tutar"])
        else:
            st.info("Gider merkezi verisi yok.")

    st.markdown("---")
    if not transactions_df.empty:
        st.subheader("Dönem Özeti")
        grafik_df = transactions_df.groupby(["tarih", "islem_turu"], as_index=False)["tutar"].sum()
        st.line_chart(grafik_df, x="tarih", y="tutar", color="islem_turu")
        st.dataframe(transactions_df.head(PAGE_SIZE_DEFAULT), use_container_width=True, hide_index=True)
    else:
        st.info("Seçilen dönem için kayıt bulunamadı.")

# -------------------------------------------------
# Yeni Kayıt
# -------------------------------------------------
elif menu == "Yeni Kayıt":
    st.subheader("Yeni Gelir / Gider Kaydı")

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
            gider_merkezi = st.selectbox(
                "Gider Merkezi",
                ["Servis", "Ofis", "Araç", "Depo", "Personel", "Satış", "Genel"],
            )
            aciklama = st.text_input("Açıklama")

        st.markdown("#### Vade / Tahsilat Bilgisi")
        v1, v2 = st.columns(2)

        with v1:
            vade_var = st.checkbox("Vade Tarihi Gir")
            vade_tarihi = st.date_input("Vade Tarihi", value=date.today()) if vade_var else None

        with v2:
            tahsilat_var = st.checkbox("Tahsilat Tarihi Gir")
            tahsilat_tarihi = st.date_input("Tahsilat Tarihi", value=date.today()) if tahsilat_var else None

        tahsilat_notu = st.text_input("Tahsilat Notu")
        notlar = st.text_area("Notlar")

        kaydet = st.form_submit_button("Kaydı Ekle", use_container_width=True)

        if kaydet:
            if not aciklama.strip():
                st.error("Açıklama zorunludur.")
            elif tutar <= 0:
                st.error("Tutar 0'dan büyük olmalıdır.")
            else:
                try:
                    execute_query(
                        """
                        INSERT INTO transactions (
                            tarih, islem_turu, kategori, alt_kategori, aciklama, tutar,
                            odeme_turu, cari_unvan, personel_adi, odeme_durumu, gider_merkezi,
                            vade_tarihi, tahsilat_tarihi, tahsilat_notu, notlar
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            tarih,
                            islem_turu,
                            kategori,
                            alt_kategori or None,
                            aciklama,
                            float(tutar),
                            odeme_turu,
                            cari_unvan or None,
                            personel_adi or None,
                            odeme_durumu,
                            gider_merkezi,
                            vade_tarihi,
                            tahsilat_tarihi,
                            tahsilat_notu or None,
                            notlar or None,
                        ],
                    )
                    clear_all_cache()
                    st.success("Kayıt başarıyla eklendi.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Kayıt eklenemedi: {e}")
# -------------------------------------------------
# Cari Hesaplar
# -------------------------------------------------
elif menu == "Cari Hesaplar":
    cari_df = transactions_df[transactions_df["cari_unvan"].fillna("") != ""] if not transactions_df.empty else pd.DataFrame()

    if not cari_df.empty:
        st.dataframe(cari_df.head(PAGE_SIZE_DEFAULT), use_container_width=True, hide_index=True)

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
        st.dataframe(cash_df.head(PAGE_SIZE_DEFAULT), use_container_width=True, hide_index=True)

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
    st.subheader("Tüm Hareketler")

    filter_options = get_filter_options()

    with st.container(border=True):
        st.markdown("#### Arama ve Filtreler")
        f1, f2, f3 = st.columns(3)

        with f1:
            search_text = st.text_input("Arama", placeholder="Açıklama, cari, personel, kategori")
            islem_filter = st.selectbox("İşlem Türü", ["Hepsi", "Gelir", "Gider"])
            kategori_filter = st.selectbox("Kategori", ["Hepsi"] + filter_options["kategori"])

        with f2:
            cari_filter = st.selectbox("Cari", ["Hepsi"] + filter_options["cari"])
            personel_filter = st.selectbox("Personel", ["Hepsi"] + filter_options["personel"])
            odeme_durumu_filter = st.selectbox("Ödeme Durumu", ["Hepsi"] + filter_options["odeme_durumu"])

        with f3:
            min_tutar = st.number_input("Min Tutar", min_value=0.0, step=100.0, value=0.0)
            max_tutar = st.number_input("Max Tutar", min_value=0.0, step=100.0, value=0.0)
            page_size = st.selectbox("Sayfa Başına Kayıt", [50, 100, 200], index=1)
            only_overdue = st.checkbox("Sadece Vadesi Geçenler")

    if "hareket_page" not in st.session_state:
        st.session_state["hareket_page"] = 0

    current_page = st.session_state["hareket_page"]

    filtered_df, total_count = get_transactions_advanced(
        rapor_tipi,
        secili_tarih,
        search_text.strip(),
        islem_filter,
        kategori_filter,
        cari_filter,
        personel_filter,
        odeme_durumu_filter,
        min_tutar,
        max_tutar,
        only_overdue,
        current_page,
        page_size,
    )

    total_pages = max((total_count - 1) // page_size + 1, 1)
    if current_page >= total_pages:
        st.session_state["hareket_page"] = max(total_pages - 1, 0)
        st.rerun()

    info1, info2, info3 = st.columns(3)
    info1.metric("Toplam Kayıt", total_count)
    info2.metric("Sayfa", f"{st.session_state['hareket_page'] + 1} / {total_pages}")
    info3.metric("Gösterilen", len(filtered_df))

    nav1, nav2, nav3 = st.columns([1, 1, 4])
    if nav1.button("◀ Önceki", disabled=st.session_state["hareket_page"] <= 0, use_container_width=True):
        st.session_state["hareket_page"] = max(st.session_state["hareket_page"] - 1, 0)
        st.rerun()

    if nav2.button("Sonraki ▶", disabled=(st.session_state["hareket_page"] + 1) >= total_pages, use_container_width=True):
        st.session_state["hareket_page"] = min(st.session_state["hareket_page"] + 1, total_pages - 1)
        st.rerun()

    if not filtered_df.empty:
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)

        secenek_df = filtered_df[["id", "tarih", "islem_turu", "kategori", "aciklama", "tutar"]].copy()
        secenek_df["etiket"] = secenek_df.apply(
            lambda r: f"#{int(r['id'])} | {str(r['tarih'])[:10]} | {r['islem_turu']} | {r['kategori']} | {r['aciklama']} | {money(r['tutar'])}",
            axis=1,
        )
        secenekler = dict(zip(secenek_df["etiket"], secenek_df["id"]))

        st.markdown("---")
        st.subheader("Kayıt İşlemleri")
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

                    ev1, ev2 = st.columns(2)
                    with ev1:
                        current_vade = pd.to_datetime(row["vade_tarihi"]).date() if pd.notna(row["vade_tarihi"]) else None
                        edit_vade_tarihi = st.date_input("Vade Tarihi", value=current_vade, key="edit_vade")
                    with ev2:
                        current_tahsilat = pd.to_datetime(row["tahsilat_tarihi"]).date() if pd.notna(row["tahsilat_tarihi"]) else None
                        edit_tahsilat_tarihi = st.date_input("Tahsilat Tarihi", value=current_tahsilat, key="edit_tahsilat")

                    edit_tahsilat_notu = st.text_input("Tahsilat Notu", value=row["tahsilat_notu"] or "")
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
                                    vade_tarihi = %s,
                                    tahsilat_tarihi = %s,
                                    tahsilat_notu = %s,
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
                                    edit_vade_tarihi,
                                    edit_tahsilat_tarihi,
                                    edit_tahsilat_notu,
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
        st.info("Filtreye uygun kayıt bulunamadı.")

st.markdown("---")
st.caption(
    "FAZ 1 eklendi: gelişmiş arama/filtre, sayfalama, kayıt düzenleme-silme, vade ve tahsilat sistemi, "
    "nakit akış özeti ve dashboard 2.0. Vergi hesabı: önce %20, sonra kalan tutarın %25'i."
)
