# streamlit_app.py  — Версия 4.1

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# ─────────────────────────────────────────────
st.set_page_config(page_title="Hotel Analytics", page_icon="🏨",
                   layout="wide", initial_sidebar_state="expanded")

# ─────────────────────────────────────────────  СТИЛИ
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=DM+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"]          { font-family:'DM Sans',sans-serif; font-size:15px; }
.stApp                              { background:#0f1117; color:#f0ece6; }
section[data-testid="stSidebar"]    { background:#161b27 !important; border-right:1px solid #2a2f3e; }
section[data-testid="stSidebar"] *  { color:#e8e4dc !important; font-size:14px !important; }
h1,h2,h3                            { font-family:'Playfair Display',serif !important; color:#ffffff !important; }
p,li,span,div                       { color:#f0ece6; }

[data-testid="metric-container"]          { background:#161b27; border:1px solid #2a2f3e; border-radius:12px; padding:18px 22px !important; }
[data-testid="metric-container"]:hover    { border-color:#c9a96e; }
[data-testid="stMetricLabel"]             { color:#c8c0b0 !important; font-size:0.78rem !important; letter-spacing:0.07em; text-transform:uppercase; }
[data-testid="stMetricValue"]             { color:#ffffff !important; font-family:'Playfair Display',serif !important; font-size:1.35rem !important; white-space:nowrap; }

[data-testid="stTabs"] button                       { font-family:'DM Sans',sans-serif !important; color:#c0b8a8 !important; font-size:0.82rem; letter-spacing:0.05em; text-transform:uppercase; }
[data-testid="stTabs"] button[aria-selected="true"] { color:#c9a96e !important; border-bottom-color:#c9a96e !important; }

[data-testid="stDataFrame"]   { border:1px solid #2a2f3e; border-radius:8px; }
[data-testid="stDataFrame"] * { font-size:14px !important; color:#ffffff !important; }

hr { border-color:#2a2f3e !important; }

.insight-box {
    background:linear-gradient(135deg,#1e2435 0%,#161b27 100%);
    border-left:3px solid #c9a96e; border-radius:0 8px 8px 0;
    padding:14px 20px; margin:10px 0 18px 0;
    font-size:0.95rem; color:#f0ece6; line-height:1.65;
}
.insight-box strong { color:#c9a96e; }
.section-title {
    font-family:'Playfair Display',serif; font-size:1.05rem;
    color:#c9a96e; letter-spacing:0.04em;
    margin:24px 0 6px 0; padding-bottom:6px; border-bottom:1px solid #2a2f3e;
}
.table-desc { font-size:0.88rem; color:#c0b8a8; margin-bottom:6px; font-style:italic; }
.badge-up   { color:#4ade80; font-weight:600; }
.badge-down { color:#f87171; font-weight:600; }
</style>
""", unsafe_allow_html=True)

DATA_PATH   = Path("data/raw")
PALETTE     = ["#c9a96e","#7ec8c8","#e07b7b","#a8c8a0","#b8a0c8","#e0b87b"]
MONTH_NAMES = {1:"Янв",2:"Фев",3:"Мар",4:"Апр",5:"Май",6:"Июн",
               7:"Июл",8:"Авг",9:"Сен",10:"Окт",11:"Ноя",12:"Дек"}

BASE_LAYOUT = dict(
    paper_bgcolor="#0f1117", plot_bgcolor="#0f1117",
    font=dict(color="#f0ece6", family="DM Sans", size=13),
    title_font=dict(family="Playfair Display", size=17, color="#ffffff"),
    colorway=PALETTE,
    xaxis=dict(gridcolor="#2a2f3e", linecolor="#2a2f3e", tickfont=dict(color="#f0ece6", size=12)),
    yaxis=dict(gridcolor="#2a2f3e", linecolor="#2a2f3e", tickfont=dict(color="#f0ece6", size=12)),
    legend=dict(font=dict(color="#f0ece6", size=12)),
)

def apply_theme(fig, title=""):
    fig.update_layout(**BASE_LAYOUT)
    if title:
        fig.update_layout(title=dict(text=title))
    fig.update_xaxes(tickfont=dict(color="#f0ece6", size=12))
    fig.update_yaxes(tickfont=dict(color="#f0ece6", size=12))
    return fig

# ── ФОРМАТИРОВАНИЕ (обычный пробел — работает везде) ──────────────────────────
def fmt_int(x):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return "—"
        return "{:,}".format(int(x)).replace(",", " ")
    except: return "—"

def fmt_money(x):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return "—"
        return "{:,}".format(int(round(x))).replace(",", " ") + " ₽"
    except: return "—"

def fmt_percent(x):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return "—"
        return f"{round(float(x), 1)} %"
    except: return "—"

def fmt_float(x, dec=1):
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)): return "—"
        return f"{round(float(x), dec)}"
    except: return "—"

# ── ЗАГРУЗКА ──────────────────────────────────────────────────────────────────
OPTION_SHEETS = {"option","options","опции","услуги","доп услуги","доп. услуги","additional","extras"}

@st.cache_data
def load_data(years):
    all_main, all_opts = [], []
    for year in years:
        yp = DATA_PATH / str(year)
        if not yp.exists(): continue
        for f in yp.rglob("*.xlsx"):
            try: all_main.append(pd.read_excel(f))
            except Exception as e: st.warning(f"Ошибка {f.name}: {e}"); continue
            try:
                xl  = pd.ExcelFile(f)
                opt = next((s for s in xl.sheet_names if s.strip().lower() in OPTION_SHEETS), None)
                if opt: all_opts.append(pd.read_excel(f, sheet_name=opt))
            except: pass

    if not all_main: st.error("Файлы не найдены в data/raw."); st.stop()
    df = pd.concat(all_main, ignore_index=True)
    options = (pd.concat(all_opts, ignore_index=True) if all_opts
               else pd.DataFrame(columns=["booking_id","service_name","service_qty","service_amount"]))

    df = df.rename(columns={
        "№ брони":"booking_id","Гость":"guest_name","Стоимость":"amount",
        "Дата бронирования":"booking_date","Заезд":"checkin_date","Выезд":"checkout_date",
        "Статус брони":"status","Объект размещения":"hotel_name",
        "Категория номера":"room_category","Дата рождения":"birth_date",
        "Источник":"source","Промокод":"promo_code",
    })
    options = options.rename(columns={
        "№ брони":"booking_id","Дополнительная услуга":"service_name",
        "Количество":"service_qty","Стоимость":"service_amount",
    })
    for col in ["booking_date","checkin_date","checkout_date","birth_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    if "service_amount" in options.columns:
        options["service_amount"] = pd.to_numeric(options["service_amount"], errors="coerce").fillna(0)

    df["nights"]      = (df["checkout_date"] - df["checkin_date"]).dt.days.clip(lower=1)
    df["lead_time"]   = (df["checkin_date"]  - df["booking_date"]).dt.days.clip(lower=0)
    df["age"]         = (datetime.now() - df["birth_date"]).dt.days / 365
    df["age_group"]   = pd.cut(df["age"], bins=[0,25,35,45,60,120],
                                labels=["до 25","26–35","36–45","46–60","60+"])
    df["lead_bucket"] = pd.cut(df["lead_time"], bins=[-1,3,14,30,60,10000],
                                labels=["0–3 дня","4–14 дней","15–30 дней","31–60 дней","60+ дней"])
    df["booking_ym"]   = df["booking_date"].dt.to_period("M")
    df["booking_year"] = df["booking_date"].dt.year
    return df, options

# ── ХЕЛПЕРЫ ───────────────────────────────────────────────────────────────────
def cancel_rate_df(data, group_col):
    grp = data.groupby(group_col, observed=True).agg(
        Всего    = ("booking_id","count"),
        Отменено = ("status", lambda x: x.str.lower().str.contains("отмен", na=False).sum())
    ).reset_index()
    grp["% отмен"] = (grp["Отменено"] / grp["Всего"] * 100).round(1)
    return grp

def insight_box(text):
    st.markdown(f'<div class="insight-box">💡 {text}</div>', unsafe_allow_html=True)

def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)

def table_desc(text):
    st.markdown(f'<div class="table-desc">📋 {text}</div>', unsafe_allow_html=True)

def pct_delta(a, b):
    if not a or a == 0: return None
    return (b - a) / a * 100

def show_cancel_table(data, group_col, top_n=None):
    """Одна строка = одно значение группы. Без склейки."""
    res = cancel_rate_df(data, group_col)
    res[group_col] = res[group_col].astype(str)
    res = res.sort_values("% отмен", ascending=False)
    if top_n:
        res = res.head(top_n)
    disp = res.copy()
    disp["Всего броней"] = disp["Всего"].apply(fmt_int)
    disp["Отменено"]     = disp["Отменено"].apply(fmt_int)
    disp["% отмен"]      = disp["% отмен"].apply(fmt_percent)
    disp = disp[[group_col, "Всего броней", "Отменено", "% отмен"]]
    st.dataframe(disp, use_container_width=True, hide_index=True,
                 column_config={group_col: st.column_config.TextColumn(width="large")})
    return res

# ═══════════════════════════════════════════════  SIDEBAR
with st.sidebar:
    st.markdown("## 🏨 Hotel Analytics")
    st.markdown("---")
    avail_years    = sorted([p.name for p in DATA_PATH.iterdir() if p.is_dir()]) if DATA_PATH.exists() else []
    selected_years = st.multiselect("Годы", avail_years, default=avail_years)
    df, options    = load_data(tuple(selected_years))
    st.markdown("---")

    # FIX #3 — analysis_date строится до фильтра месяцев
    analysis_mode       = st.radio("Анализировать по", ["Дате бронирования","Дате заезда"])
    df["analysis_date"] = df["booking_date"] if analysis_mode == "Дате бронирования" else df["checkin_date"]

    all_months     = sorted(df["analysis_date"].dt.month.dropna().unique().astype(int))
    month_opts     = [f"{MONTH_NAMES.get(m,m)} ({m})" for m in all_months]
    sel_months     = st.multiselect("Месяцы", month_opts, default=month_opts)
    sel_month_nums = [int(m.split("(")[1].rstrip(")")) for m in sel_months] if sel_months else all_months
    df = df[df["analysis_date"].dt.month.isin(sel_month_nums)].copy()

    all_hotels   = sorted(df["hotel_name"].dropna().unique())
    hotel_filter = st.multiselect("Отели", all_hotels, default=all_hotels)
    df = df[df["hotel_name"].isin(hotel_filter)].copy()
    st.markdown("---")
    st.caption("Версия 4.1 · Hotel Analytics Platform")

confirmed = df[df["status"].str.lower().str.contains("актив", na=False)].copy()
cancelled = df[df["status"].str.lower().str.contains("отмен", na=False)].copy()
df["Месяц заезда"]        = df["checkin_date"].dt.month.map(MONTH_NAMES)
confirmed["Месяц заезда"] = confirmed["checkin_date"].dt.month.map(MONTH_NAMES)

# ═══════════════════════════════════════════════  ВКЛАДКИ
tabs = st.tabs(["📊 Обзор","🏨 Отели","👥 Гости и LTV",
                "🧠 Поведение гостей","❌ Отмены","🛎 Доп. услуги"])

# ══════════════════════════════════════════════  ВК.1 — ОБЗОР
with tabs[0]:
    st.markdown("# Обзор")

    total_bookings = len(df)
    revenue        = confirmed["amount"].sum()
    cancel_rate_v  = len(cancelled)/len(df)*100 if len(df)>0 else 0
    avg_lead       = confirmed["lead_time"].mean()
    unique_guests  = confirmed["guest_name"].nunique()
    avg_check      = confirmed["amount"].mean()
    avg_nights     = confirmed["nights"].mean()
    repeat_vals    = confirmed["guest_name"].value_counts()
    repeat_rate    = len(repeat_vals[repeat_vals>1])/len(repeat_vals)*100 if len(repeat_vals)>0 else 0

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Бронирований",     fmt_int(total_bookings))
    c2.metric("Выручка",          fmt_money(revenue))
    c3.metric("% отмен",          fmt_percent(cancel_rate_v))
    c4.metric("Ср. Lead Time",    f"{fmt_float(avg_lead)} дн.")

    c5,c6,c7,c8 = st.columns(4)
    c5.metric("Уникальных гостей",  fmt_int(unique_guests))
    c6.metric("Ср. чек брон-ия",    fmt_money(avg_check))       # FIX #4
    c7.metric("Средние ночи",       f"{fmt_float(avg_nights)} н.")
    c8.metric("Доля повторных",     fmt_percent(repeat_rate))

    st.markdown("<small style='color:#a09880'>Ср. чек брон-ия = выручка ÷ кол-во подтверждённых броней &nbsp;|&nbsp; Средние ночи = среднее (выезд − заезд)</small>", unsafe_allow_html=True)
    st.divider()

    section("Динамика бронирований по сети")
    monthly = df.groupby(df["booking_date"].dt.to_period("M"))["booking_id"].count().reset_index()
    monthly.columns = ["Месяц","Бронирования"]
    monthly["Месяц"] = monthly["Месяц"].astype(str)
    fig = px.line(monthly, x="Месяц", y="Бронирования", markers=True)
    fig.update_traces(line_color="#c9a96e", marker_color="#c9a96e", line_width=2)
    apply_theme(fig,"Бронирования по месяцам (вся сеть)")
    st.plotly_chart(fig, use_container_width=True)

    section("Динамика бронирований по каждому отелю")
    hm = df.groupby([df["booking_date"].dt.to_period("M"),"hotel_name"])["booking_id"].count().reset_index()
    hm.columns = ["Месяц","Отель","Бронирования"]
    hm["Месяц"] = hm["Месяц"].astype(str)
    fig2 = px.line(hm, x="Месяц", y="Бронирования", color="Отель", markers=True,
                   color_discrete_sequence=PALETTE)
    apply_theme(fig2,"Бронирования по месяцам — по отелям")
    st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    section("Сравнение двух периодов")
    avail_years_list = sorted(df["booking_year"].dropna().unique().astype(int))
    if len(avail_years_list) >= 2:
        cp1,cp2 = st.columns(2)
        with cp1:
            p1s = st.date_input("Период 1 — с",  pd.Timestamp(f"{avail_years_list[-2]}-01-01"), key="ov_p1s")
            p1e = st.date_input("Период 1 — по", pd.Timestamp(f"{avail_years_list[-2]}-12-31"), key="ov_p1e")
        with cp2:
            p2s = st.date_input("Период 2 — с",  pd.Timestamp(f"{avail_years_list[-1]}-01-01"), key="ov_p2s")
            p2e = st.date_input("Период 2 — по", pd.Timestamp(f"{avail_years_list[-1]}-12-31"), key="ov_p2e")

        p1 = df[(df["booking_date"]>=pd.Timestamp(p1s))&(df["booking_date"]<=pd.Timestamp(p1e))]
        p2 = df[(df["booking_date"]>=pd.Timestamp(p2s))&(df["booking_date"]<=pd.Timestamp(p2e))]
        p1c = p1[p1["status"].str.lower().str.contains("актив",na=False)]
        p2c = p2[p2["status"].str.lower().str.contains("актив",na=False)]

        metrics_cmp = {
            "Бронирований": (len(p1),             len(p2),             False),
            "Выручка":      (p1c["amount"].sum(),  p2c["amount"].sum(), False),
            "Ср. чек":      (p1c["amount"].mean(), p2c["amount"].mean(),False),
            "% отмен":      (
                len(p1[p1["status"].str.lower().str.contains("отмен",na=False)])/len(p1)*100 if len(p1) else 0,
                len(p2[p2["status"].str.lower().str.contains("отмен",na=False)])/len(p2)*100 if len(p2) else 0,
                True),
        }
        cmp_cols = st.columns(4)
        for idx,(name,(v1,v2,inv)) in enumerate(metrics_cmp.items()):
            d   = pct_delta(v1,v2)
            is_money = "чек" in name.lower() or "выруч" in name.lower()
            is_pct   = "%" in name
            v1s = fmt_money(v1) if is_money else (fmt_percent(v1) if is_pct else fmt_int(v1))
            v2s = fmt_money(v2) if is_money else (fmt_percent(v2) if is_pct else fmt_int(v2))
            badge = ""
            if d is not None:
                good  = (d>0 and not inv) or (d<0 and inv)
                color = "#4ade80" if good else "#f87171"
                sign  = "+" if d>0 else ""
                badge = f'<span style="color:{color};font-size:0.8rem">{"▲" if d>0 else "▼"} {sign}{d:.1f}%</span>'
            cmp_cols[idx].markdown(f"""
            <div style="background:#161b27;border:1px solid #2a2f3e;border-radius:12px;padding:16px 20px">
              <div style="color:#a09880;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.08em">{name}</div>
              <div style="color:#ffffff;font-size:0.9rem;margin:6px 0">{v1s} → {v2s}</div>
              {badge}
            </div>""", unsafe_allow_html=True)

        p1m = p1.groupby(p1["booking_date"].dt.month)["booking_id"].count().reset_index()
        p2m = p2.groupby(p2["booking_date"].dt.month)["booking_id"].count().reset_index()
        p1m.columns=["Месяц","П1"]; p2m.columns=["Месяц","П2"]
        cmp_m = pd.merge(p1m,p2m,on="Месяц",how="outer").fillna(0)
        cmp_m["МесяцНазв"] = cmp_m["Месяц"].map(MONTH_NAMES)
        fig_cmp = go.Figure()
        fig_cmp.add_trace(go.Bar(name="Период 1",x=cmp_m["МесяцНазв"],y=cmp_m["П1"],marker_color="#7ec8c8",opacity=0.85))
        fig_cmp.add_trace(go.Bar(name="Период 2",x=cmp_m["МесяцНазв"],y=cmp_m["П2"],marker_color="#c9a96e",opacity=0.85))
        fig_cmp.update_layout(barmode="group")
        apply_theme(fig_cmp,"Сравнение периодов — бронирования по месяцам")
        st.plotly_chart(fig_cmp, use_container_width=True)

        dr = pct_delta(p1c["amount"].sum(), p2c["amount"].sum())
        db = pct_delta(len(p1), len(p2))
        parts=[]
        if dr: parts.append(f"Выручка {'выросла' if dr>0 else 'упала'} на <strong>{abs(dr):.1f}%</strong>.")
        if db: parts.append(f"Бронирований стало {'больше' if db>0 else 'меньше'} на <strong>{abs(db):.1f}%</strong>.")
        if parts: insight_box(" ".join(parts))
    else:
        st.info("Для сравнения периодов нужны данные минимум за 2 разных года.")

# ══════════════════════════════════════════════  ВК.2 — ОТЕЛИ
with tabs[1]:
    st.markdown("# Отели")

    hotel_stats = confirmed.groupby("hotel_name").agg(
        Бронирования=("booking_id","count"),
        Выручка=("amount","sum"),
        Ср_чек=("amount","mean"),
        Ср_ночи=("nights","mean"),
    ).reset_index().sort_values("Выручка",ascending=False)
    hotel_stats.insert(0,"№",range(1,len(hotel_stats)+1))

    disp = hotel_stats.copy()
    disp["Бронирования"]    = disp["Бронирования"].apply(fmt_int)
    disp["Выручка"]         = disp["Выручка"].apply(fmt_money)
    disp["Ср. чек брон-ия"] = disp["Ср_чек"].apply(fmt_money)       # FIX #5,6
    disp["Средние ночи"]    = disp["Ср_ночи"].apply(lambda x: fmt_float(x)+" н.")
    disp = disp[["№","hotel_name","Бронирования","Выручка","Ср. чек брон-ия","Средние ночи"]].rename(columns={"hotel_name":"Отель"})

    table_desc("Все подтверждённые брони. Ср. чек брон-ия = выручка ÷ кол-во броней. Средние ночи = среднее кол-во ночей за одну бронь.")
    st.dataframe(disp, use_container_width=True, hide_index=True,
                 column_config={
                     "Отель":           st.column_config.TextColumn(width="large"),
                     "Выручка":         st.column_config.TextColumn(width="medium"),
                     "Ср. чек брон-ия": st.column_config.TextColumn(width="medium"),
                 })
    st.divider()

    # FIX #7 — сумма внутри столбца, не обрезается
    section("Выручка по отелям")
    bar_data = hotel_stats.sort_values("Выручка").copy()
    bar_data["Выручка_текст"] = bar_data["Выручка"].apply(fmt_money)
    fig_h = px.bar(bar_data, x="Выручка", y="hotel_name", orientation="h",
                   color_discrete_sequence=["#c9a96e"], text="Выручка_текст")
    fig_h.update_traces(
        textposition="inside",
        insidetextanchor="end",
        textfont=dict(color="#0f1117", size=12),
        cliponaxis=False,
    )
    apply_theme(fig_h,"Выручка по отелям (подтверждённые брони)")
    st.plotly_chart(fig_h, use_container_width=True)

# ══════════════════════════════════════════════  ВК.3 — ГОСТИ И LTV
with tabs[2]:
    st.markdown("# Гости и LTV")

    st.markdown("""
    <div class='insight-box'>
    <strong>Зачем маркетингу нужен LTV?</strong><br>
    LTV (Lifetime Value) — общая выручка от одного гостя за всё время.
    Зная LTV, вы понимаете: <em>кому давать скидку</em> (лояльным — окупится),
    <em>куда тратить рекламный бюджет</em> (привлекать тех, кто вернётся),
    <em>кого удерживать звонком/письмом</em>, <em>как сегментировать базу</em> для email/SMS-кампаний.
    </div>""", unsafe_allow_html=True)

    ltv = confirmed.groupby("guest_name")["amount"].sum().reset_index()
    ltv.columns = ["Гость","LTV"]
    ltv["Сегмент"] = pd.cut(ltv["LTV"],
        bins=[0,20000,60000,120000,float("inf")],
        labels=["до 20 000","20 000–60 000","60 000–120 000","120 000+"])

    section("Распределение LTV — вся сеть")
    fig_ltv = px.histogram(ltv, x="LTV", nbins=50, color_discrete_sequence=["#c9a96e"])
    apply_theme(fig_ltv,"Распределение LTV гостей (вся сеть)")
    st.plotly_chart(fig_ltv, use_container_width=True)

    seg_counts = ltv["Сегмент"].value_counts().sort_index().reset_index()
    seg_counts.columns = ["Сегмент LTV","Гостей"]
    fig_seg = px.bar(seg_counts, x="Сегмент LTV", y="Гостей", color_discrete_sequence=PALETTE)
    apply_theme(fig_seg,"Сегменты гостей по LTV")
    st.plotly_chart(fig_seg, use_container_width=True)

    top10pct = ltv.nlargest(max(1,int(len(ltv)*0.1)),"LTV")
    insight_box(
        f"Средний LTV гостя — <strong>{fmt_money(ltv['LTV'].mean())}</strong>. "
        f"Топ-10% гостей приносят <strong>{fmt_percent(top10pct['LTV'].sum()/ltv['LTV'].sum()*100)}</strong> от общей выручки."
    )

    section("Средний LTV по отелям")
    ltv_hotel = confirmed.groupby(["hotel_name","guest_name"])["amount"].sum().reset_index()
    ltv_hotel.columns = ["Отель","Гость","LTV"]
    ltv_avg   = ltv_hotel.groupby("Отель")["LTV"].mean().reset_index()
    ltv_avg.columns = ["Отель","Средний LTV"]
    fig_lh = px.bar(ltv_avg.sort_values("Средний LTV",ascending=False),
                    x="Отель", y="Средний LTV", color_discrete_sequence=PALETTE)
    apply_theme(fig_lh,"Средний LTV гостя по отелям")
    st.plotly_chart(fig_lh, use_container_width=True)

    # FIX #8,9 — чёткая таблица: один гость = одна строка, + колонка Отели
    guest_hotels = (confirmed.groupby("guest_name")["hotel_name"]
                    .apply(lambda x: ", ".join(sorted(x.dropna().unique())))
                    .reset_index())
    guest_hotels.columns = ["Гость","Отели"]

    section("Топ-20 гостей по совокупной выручке")
    table_desc("Каждая строка = один уникальный гость. LTV = сумма всех его бронирований. В колонке Отели — где именно останавливался.")
    top20 = ltv.nlargest(20,"LTV").merge(guest_hotels, on="Гость", how="left").copy()
    top20.insert(0,"№",range(1,len(top20)+1))
    top20_disp = top20.copy()
    top20_disp["LTV"] = top20_disp["LTV"].apply(fmt_money)
    top20_disp["Сегмент"] = top20_disp["Сегмент"].astype(str)
    st.dataframe(top20_disp[["№","Гость","LTV","Сегмент","Отели"]], use_container_width=True, hide_index=True,
                 column_config={
                     "Гость": st.column_config.TextColumn(width="large"),
                     "Отели": st.column_config.TextColumn(width="large"),
                     "LTV":   st.column_config.TextColumn(width="medium"),
                 })

    # FIX #10 — топ-20 по сегментам
    for seg_label, seg_title in [("20 000–60 000","Топ-20 гостей — сегмент 20 000–60 000 ₽"),
                                   ("60 000–120 000","Топ-20 гостей — сегмент 60 000–120 000 ₽")]:
        section(seg_title)
        table_desc(f"Гости с совокупной выручкой {seg_label} ₽, отсортированные по убыванию.")
        seg_g = ltv[ltv["Сегмент"].astype(str)==seg_label].nlargest(20,"LTV").merge(guest_hotels,on="Гость",how="left")
        if seg_g.empty:
            st.info(f"Гостей в сегменте {seg_label} не найдено.")
        else:
            seg_g = seg_g.copy()
            seg_g.insert(0,"№",range(1,len(seg_g)+1))
            seg_g["LTV"] = seg_g["LTV"].apply(fmt_money)
            st.dataframe(seg_g[["№","Гость","LTV","Отели"]], use_container_width=True, hide_index=True,
                         column_config={
                             "Гость": st.column_config.TextColumn(width="large"),
                             "Отели": st.column_config.TextColumn(width="large"),
                         })

    section("Сегментация гостей: разовые / повторные / лояльные")
    visits  = confirmed.groupby("guest_name")["booking_id"].count()
    seg_map = visits.apply(lambda x: "Разовые (1 визит)" if x==1 else ("Повторные (2 визита)" if x==2 else "Лояльные (3+)"))
    seg_df  = seg_map.value_counts().reset_index()
    seg_df.columns = ["Сегмент","Гостей"]
    fig_pie = px.pie(seg_df, names="Сегмент", values="Гостей", color_discrete_sequence=PALETTE, hole=0.4)
    apply_theme(fig_pie,"Сегментация гостей по частоте визитов")
    st.plotly_chart(fig_pie, use_container_width=True)

    if "promo_code" in confirmed.columns:
        section("Анализ промокодов")
        promo = confirmed[confirmed["promo_code"].notna()&(confirmed["promo_code"].astype(str).str.strip()!="")]
        if len(promo)>0:
            ps = promo.groupby("promo_code").agg(
                Бронирований=("booking_id","count"),Выручка=("amount","sum"),Ср_чек=("amount","mean")
            ).reset_index().sort_values("Выручка",ascending=False)
            disp_p = ps.copy()
            disp_p["Бронирований"] = disp_p["Бронирований"].apply(fmt_int)
            disp_p["Выручка"]      = disp_p["Выручка"].apply(fmt_money)
            disp_p["Ср. чек"]      = disp_p["Ср_чек"].apply(fmt_money)
            st.dataframe(disp_p[["promo_code","Бронирований","Выручка","Ср. чек"]].rename(columns={"promo_code":"Промокод"}),
                         use_container_width=True, hide_index=True)
            fig_pr = px.bar(ps.head(10).sort_values("Выручка"),x="Выручка",y="promo_code",
                            orientation="h",color_discrete_sequence=["#7ec8c8"])
            apply_theme(fig_pr,"Топ-10 промокодов по выручке")
            st.plotly_chart(fig_pr, use_container_width=True)
            best = ps.iloc[0]
            d = pct_delta(confirmed["amount"].mean(), best["Ср_чек"])
            insight_box(f"Лучший промокод <strong>{best['promo_code']}</strong>: "
                        f"<strong>{fmt_money(best['Выручка'])}</strong> выручки, "
                        f"ср. чек {'выше' if (d or 0)>0 else 'ниже'} среднего на <strong>{abs(d or 0):.1f}%</strong>.")
        else:
            st.info("Бронирований с промокодами не найдено.")

# ══════════════════════════════════════════════  ВК.4 — ПОВЕДЕНИЕ ГОСТЕЙ
with tabs[3]:
    st.markdown("# Поведение гостей")

    section("Возраст × Категория номера — Топ-5 по каждому отелю")
    for hotel in sorted(confirmed["hotel_name"].dropna().unique()):
        hdf   = confirmed[confirmed["hotel_name"]==hotel]
        top5  = hdf["room_category"].value_counts().head(5).index
        pivot = hdf[hdf["room_category"].isin(top5)].pivot_table(
            index="age_group", columns="room_category", values="booking_id", aggfunc="count"
        ).fillna(0)
        if pivot.empty: continue
        fig_hm = px.imshow(pivot, text_auto=True,
                           color_continuous_scale=["#161b27","#c9a96e"], aspect="auto")
        apply_theme(fig_hm, f"{hotel} — Возраст × Топ-5 категорий")
        st.plotly_chart(fig_hm, use_container_width=True)
        top_age = pivot.sum(axis=1).idxmax()
        top_cat = pivot.sum(axis=0).idxmax()
        insight_box(f"<strong>{hotel}</strong>: самая активная возрастная группа — <strong>{top_age}</strong>. "
                    f"Самая популярная категория — <strong>{top_cat}</strong>. "
                    f"Это ядро аудитории для таргетированных предложений и спецтарифов.")

    st.divider()

    # FIX #11,12 — Lead Time по каждому отелю отдельной вкладкой + выводы
    section("Lead Time по каждому отелю")
    hotels_sorted = sorted(confirmed["hotel_name"].dropna().unique())
    lt_tabs = st.tabs(hotels_sorted)

    for i, hotel in enumerate(hotels_sorted):
        with lt_tabs[i]:
            hlt = confirmed[confirmed["hotel_name"]==hotel].copy()

            # Средний Lead Time по месяцу заезда
            lt_m = hlt.groupby(hlt["checkin_date"].dt.month)["lead_time"].mean().reset_index()
            lt_m.columns = ["Месяц","Ср. Lead Time"]
            lt_m["МесяцНазв"] = lt_m["Месяц"].map(MONTH_NAMES)

            fig_ltm = px.bar(lt_m, x="МесяцНазв", y="Ср. Lead Time",
                             color_discrete_sequence=["#7ec8c8"])
            apply_theme(fig_ltm, f"Средний Lead Time по месяцу заезда — {hotel}")
            st.plotly_chart(fig_ltm, use_container_width=True)

            if len(lt_m)>0:
                peak = lt_m.loc[lt_m["Ср. Lead Time"].idxmax()]
                low  = lt_m.loc[lt_m["Ср. Lead Time"].idxmin()]
                insight_box(
                    f"Гости бронируют заранее всего больше перед <strong>{peak['МесяцНазв']}</strong> — "
                    f"в среднем за <strong>{peak['Ср. Lead Time']:.0f} дней</strong>. "
                    f"Запускайте рекламу за <strong>{int(peak['Ср. Lead Time'])+14} дней</strong> до этого месяца. "
                    f"Наименьший lead time — перед <strong>{low['МесяцНазв']}</strong> "
                    f"({low['Ср. Lead Time']:.0f} дн.) — здесь эффективны предложения «горящих» броней."
                )

            # Гистограмма
            fig_lth = px.histogram(hlt[hlt["lead_time"]<=200], x="lead_time", nbins=50,
                                   color_discrete_sequence=["#c9a96e"],
                                   labels={"lead_time":"Дней до заезда"})
            apply_theme(fig_lth, f"Распределение Lead Time — {hotel}")
            st.plotly_chart(fig_lth, use_container_width=True)

            # Сегменты
            bk = hlt["lead_bucket"].value_counts().sort_index().reset_index()
            bk.columns = ["Сегмент","Бронирований"]
            fig_bk = px.bar(bk, x="Сегмент", y="Бронирований", color_discrete_sequence=PALETTE)
            apply_theme(fig_bk, f"Сегменты Lead Time — {hotel}")
            st.plotly_chart(fig_bk, use_container_width=True)

            if len(bk)>0:
                top_seg = bk.loc[bk["Бронирований"].idxmax(),"Сегмент"]
                top_pct = bk["Бронирований"].max()/bk["Бронирований"].sum()*100
                insight_box(
                    f"Самый частый сегмент — <strong>{top_seg}</strong> ({top_pct:.0f}% всех броней). "
                    f"Для невозвратных тарифов и депозитов ориентируйтесь именно на этот горизонт."
                )

# ══════════════════════════════════════════════  ВК.5 — ОТМЕНЫ
with tabs[4]:
    st.markdown("# Анализ отмен")

    avail_years_c = sorted(df["booking_year"].dropna().unique().astype(int))
    use_cmp = st.checkbox("Включить сравнение двух периодов", value=len(avail_years_c)>=2)

    if use_cmp and len(avail_years_c)>=2:
        cc1,cc2 = st.columns(2)
        with cc1:
            cp1s = st.date_input("Период 1 — с",  pd.Timestamp(f"{avail_years_c[-2]}-01-01"), key="c_p1s")
            cp1e = st.date_input("Период 1 — по", pd.Timestamp(f"{avail_years_c[-2]}-12-31"), key="c_p1e")
        with cc2:
            cp2s = st.date_input("Период 2 — с",  pd.Timestamp(f"{avail_years_c[-1]}-01-01"), key="c_p2s")
            cp2e = st.date_input("Период 2 — по", pd.Timestamp(f"{avail_years_c[-1]}-12-31"), key="c_p2e")

        p1df = df[(df["booking_date"]>=pd.Timestamp(cp1s))&(df["booking_date"]<=pd.Timestamp(cp1e))].copy()
        p2df = df[(df["booking_date"]>=pd.Timestamp(cp2s))&(df["booking_date"]<=pd.Timestamp(cp2e))].copy()
        for fr in [p1df,p2df]:
            fr["Месяц заезда"] = fr["checkin_date"].dt.month.map(MONTH_NAMES)
            fr["age_group"]    = fr["age_group"].astype(str)
            fr["lead_bucket"]  = fr["lead_bucket"].astype(str)
        periods   = {"Период 1": p1df, "Период 2": p2df}
        p_labels  = ["Период 1","Период 2"]
    else:
        dfc = df.copy()
        dfc["age_group"]   = dfc["age_group"].astype(str)
        dfc["lead_bucket"] = dfc["lead_bucket"].astype(str)
        periods  = {"Весь период": dfc}
        p_labels = ["Весь период"]

    def plot_cancel_block(group_col, block_title, table_hint, top_n=None):
        section(block_title)

        frames=[]
        for pname,pdata in periods.items():
            r = cancel_rate_df(pdata, group_col)
            r["Период"] = pname
            frames.append(r)
        combined = pd.concat(frames, ignore_index=True)
        combined[group_col] = combined[group_col].astype(str)

        if top_n:
            last_pname = p_labels[-1]
            top_vals = (combined[combined["Период"]==last_pname]
                        .sort_values("% отмен",ascending=False)
                        .head(top_n)[group_col].tolist())
            combined = combined[combined[group_col].isin(top_vals)]

        # Понятный бар-чарт — только % отмен, цвет по периоду
        fig = px.bar(combined, x=group_col, y="% отмен", color="Период",
                     barmode="group", color_discrete_sequence=["#7ec8c8","#c9a96e"],
                     text=combined["% отмен"].apply(lambda v: f"{v:.1f}%"))
        fig.update_traces(textposition="outside", textfont=dict(color="#f0ece6",size=11))
        apply_theme(fig, block_title)
        st.plotly_chart(fig, use_container_width=True)

        # Таблица последнего периода
        table_desc(table_hint)
        res = show_cancel_table(list(periods.values())[-1], group_col, top_n=top_n)

        # Авто-вывод
        if len(res)>0:
            worst = res.sort_values("% отмен",ascending=False).iloc[0]
            if len(periods)==2:
                r1 = frames[0]; r2 = frames[1]
                m1 = r1[r1[group_col].astype(str)==str(worst[group_col])]["% отмен"].values
                m2 = r2[r2[group_col].astype(str)==str(worst[group_col])]["% отмен"].values
                if len(m1)>0 and len(m2)>0:
                    delta_c = m2[0]-m1[0]
                    insight_box(
                        f"Лидер по отменам — <strong>{worst[group_col]}</strong>: "
                        f"Период 1: {fmt_percent(m1[0])} → Период 2: <strong>{fmt_percent(m2[0])}</strong> "
                        f"({'▲ рост' if delta_c>0 else '▼ снижение'} на {abs(delta_c):.1f} п.п.). "
                        f"Всего отменено <strong>{fmt_int(worst['Отменено'])}</strong> из <strong>{fmt_int(worst['Всего'])}</strong> броней."
                    )
                    return
            insight_box(
                f"Наибольший % отмен — <strong>{worst[group_col]}</strong>: "
                f"<strong>{fmt_percent(worst['% отмен'])}</strong> "
                f"({fmt_int(worst['Отменено'])} отменено из {fmt_int(worst['Всего'])} броней)."
            )

    plot_cancel_block(
        "Месяц заезда",
        "% отмен по месяцу заезда",
        "Каждая строка = один месяц заезда. Показывает, в каком месяце гости чаще всего отменяют бронь. Сортировка по убыванию % отмен.",
    )
    plot_cancel_block(
        "room_category",
        "% отмен по категории номера (Топ-10)",
        "Каждая строка = одна категория номера. Показывает 10 категорий с наибольшим процентом отмен.",
        top_n=10,
    )
    plot_cancel_block(
        "age_group",
        "% отмен по возрастной группе",
        "Каждая строка = одна возрастная группа. Помогает понять, какой возраст чаще отменяет бронирования.",
    )
    plot_cancel_block(
        "lead_bucket",
        "% отмен по Lead Time",
        "Каждая строка = диапазон «за сколько дней до заезда сделана бронь». Помогает решить, где вводить невозвратные тарифы или депозит.",
    )
    plot_cancel_block(
        "source",
        "% отмен по источнику бронирования",
        "Каждая строка = один канал/источник. Показывает, откуда приходят гости с наибольшим процентом отмен.",
    )

    # Lead Time × Отмены
    section("Lead Time × Отмены — совмещённый анализ")
    table_desc("Столбцы = количество броней в сегменте. Линия = % отмен. Позволяет сразу видеть и объём, и риск отмен.")
    lt_c = df.copy()
    lt_c["Отменено"]    = lt_c["status"].str.lower().str.contains("отмен",na=False)
    lt_c["lead_bucket"] = lt_c["lead_bucket"].astype(str)
    lt_g = lt_c.groupby("lead_bucket",observed=True).agg(Всего=("booking_id","count"),Отменено=("Отменено","sum")).reset_index()
    lt_g["% отмен"] = (lt_g["Отменено"]/lt_g["Всего"]*100).round(1)

    fig_ltc = go.Figure()
    fig_ltc.add_trace(go.Bar(x=lt_g["lead_bucket"],y=lt_g["Всего"],name="Всего броней",
                              marker_color="#7ec8c8",opacity=0.6))
    fig_ltc.add_trace(go.Scatter(x=lt_g["lead_bucket"],y=lt_g["% отмен"],name="% отмен",
                                  yaxis="y2",line=dict(color="#e07b7b",width=2),mode="lines+markers"))
    fig_ltc.update_layout(
        yaxis2=dict(overlaying="y",side="right",title="% отмен",
                    gridcolor="#2a2f3e",tickfont=dict(color="#f0ece6"),title_font=dict(color="#f0ece6")),
        yaxis=dict(title="Бронирований",title_font=dict(color="#f0ece6")),
    )
    apply_theme(fig_ltc,"Lead Time: количество броней и % отмен")
    st.plotly_chart(fig_ltc, use_container_width=True)

    if len(lt_g)>0:
        worst_lt  = lt_g.loc[lt_g["% отмен"].idxmax()]
        worst_src_df = cancel_rate_df(df,"source").sort_values("% отмен",ascending=False)
        msg = (f"Самый высокий % отмен — сегмент <strong>{worst_lt['lead_bucket']}</strong> "
               f"({worst_lt['% отмен']:.1f}%).")
        if len(worst_src_df)>0:
            ws = worst_src_df.iloc[0]
            msg += f" По источникам лидер — <strong>{ws['source']}</strong> ({ws['% отмен']:.1f}%)."
        msg += " Рекомендация: невозвратные тарифы и депозиты эффективны для броней <strong>более 30 дней</strong> до заезда."
        insight_box(msg)

# ══════════════════════════════════════════════  ВК.6 — ДОП. УСЛУГИ
with tabs[5]:
    st.markdown("# Дополнительные услуги")

    if options.empty or "service_name" not in options.columns or "service_amount" not in options.columns:
        st.info("Данные по дополнительным услугам не найдены. "
                "Проверьте, что в Excel-файлах есть лист с одним из названий: "
                "Option, Options, Опции, Услуги, Доп. услуги.")
    else:
        options["service_amount"] = pd.to_numeric(options["service_amount"],errors="coerce").fillna(0)
        sr = (options.groupby("service_name")
              .agg(Выручка=("service_amount","sum"),Количество=("service_qty","sum"))
              .reset_index().sort_values("Выручка",ascending=False))

        section("Топ-10 услуг по выручке")
        fig_sv = px.bar(sr.head(10).sort_values("Выручка"),x="Выручка",y="service_name",
                        orientation="h",color_discrete_sequence=["#c9a96e"])
        apply_theme(fig_sv,"Топ-10 дополнительных услуг по выручке")
        st.plotly_chart(fig_sv, use_container_width=True)

        section("Топ-10 услуг по количеству")
        fig_sq = px.bar(sr.sort_values("Количество",ascending=False).head(10).sort_values("Количество"),
                        x="Количество",y="service_name",orientation="h",color_discrete_sequence=["#7ec8c8"])
        apply_theme(fig_sq,"Топ-10 дополнительных услуг по количеству")
        st.plotly_chart(fig_sq, use_container_width=True)

        section("Сводная таблица всех услуг")
        table_desc("Каждая строка = одна услуга. Количество = сколько раз продана. Выручка = суммарная стоимость.")
        disp_s = sr.copy()
        disp_s["Выручка"]    = disp_s["Выручка"].apply(fmt_money)
        disp_s["Количество"] = disp_s["Количество"].apply(fmt_int)
        st.dataframe(disp_s[["service_name","Количество","Выручка"]].rename(columns={"service_name":"Услуга"}),
                     use_container_width=True, hide_index=True,
                     column_config={"Услуга": st.column_config.TextColumn(width="large")})

        top1 = sr.iloc[0]
        insight_box(
            f"Лидер по выручке — <strong>{top1['service_name']}</strong>: "
            f"<strong>{fmt_money(top1['Выручка'])}</strong>. "
            f"Топ-3 услуги обеспечивают "
            f"<strong>{fmt_percent(sr.head(3)['Выручка'].sum()/sr['Выручка'].sum()*100)}</strong> "
            f"от общей выручки по доп. услугам."
        )
