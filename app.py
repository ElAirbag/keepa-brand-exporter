"""
Keepa Brand Exporter — Pangaea Commerce
Streamlit app: input brand name → fetch ASINs via Keepa API → download CSV
for use with the Lead Gen Pipeline (pipeline.py)

Deploy: streamlit run app.py
Cloud:  push to GitHub → connect on share.streamlit.io → set KEEPA_API_KEY secret
"""

import streamlit as st
import pandas as pd
import keepa
import os
import io
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

MARKETPLACES = {
    "Amazon.de (Germany)":  3,
    "Amazon.com (US)":      1,
    "Amazon.co.uk (UK)":    2,
    "Amazon.fr (France)":   4,
    "Amazon.it (Italy)":    9,
    "Amazon.es (Spain)":    10,
}

BATCH_SIZE   = 100   # ASINs per Keepa product query
MAX_ASINS    = 5000  # Safety cap to prevent accidental token drain


# ─────────────────────────────────────────────
# API KEY HANDLING
# ─────────────────────────────────────────────

def get_api_key():
    """Read API key from Streamlit secrets (cloud) or env var (local)."""
    try:
        return st.secrets["KEEPA_API_KEY"]
    except Exception:
        return os.environ.get("KEEPA_API_KEY", "")


# ─────────────────────────────────────────────
# KEEPA LOGIC
# ─────────────────────────────────────────────

@st.cache_resource(ttl=300)
def get_keepa_api(api_key):
    """Initialise Keepa API client (cached for 5 min to avoid re-auth overhead)."""
    return keepa.Keepa(api_key)


def fetch_asins_for_brand(api, brand: str, bsr_limit: int, domain: int) -> list[str]:
    """
    Use Keepa Product Finder to get all ASINs for the brand
    with current BSR <= bsr_limit.
    Returns a list of ASIN strings.
    """
    product_parm = {
        "brand":          [brand],
        "salesRank_lte":  bsr_limit,
        "domain":         domain,
    }
    asins = api.product_finder(product_parm)
    return asins if asins is not None else []


def build_category_path(category_tree: list) -> str:
    """
    Convert Keepa's categoryTree list of dicts to a slash-separated path string.
    e.g. "Kueche, Haushalt & Wohnen / Kategorien / Kueche, Kochen & Backen / ..."
    The pipeline's parse_category_paths() will handle this fine.
    """
    if not category_tree:
        return ""
    return " / ".join(node.get("name", "") for node in category_tree)


def fetch_product_details(api, asins: list, domain: int,
                           progress_bar, status_text) -> pd.DataFrame:
    """
    Fetch product metadata for a list of ASINs in batches.
    Returns a DataFrame with ASIN, Parent_ASIN, Brand, Title, Category, BSR, Price.
    """
    rows = []
    total   = len(asins)
    batches = [asins[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]

    for batch_idx, batch in enumerate(batches):
        pct = batch_idx / len(batches)
        progress_bar.progress(pct)
        status_text.text(
            f"Fetching product data... batch {batch_idx + 1}/{len(batches)} "
            f"({batch_idx * BATCH_SIZE}/{total} ASINs)"
        )

        try:
            products = api.query(
                batch,
                domain=domain,
                history=False,
                buybox=False,
            )
        except Exception as e:
            st.warning(f"Batch {batch_idx + 1} failed: {e}. Skipping.")
            continue

        if products is None:
            continue

        for p in products:
            cat_tree  = p.get("categoryTree") or []
            cat_path  = build_category_path(cat_tree)
            all_cat_ids = p.get("categories") or []

            stats        = p.get("stats") or {}
            current_vals = stats.get("current") or []
            bsr = None
            if len(current_vals) > 3 and current_vals[3] is not None and current_vals[3] != -1:
                bsr = current_vals[3]

            price = None
            if len(current_vals) > 0 and current_vals[0] is not None and current_vals[0] != -1:
                price = current_vals[0] / 100.0

            rows.append({
                "ASIN":        p.get("asin", ""),
                "Parent_ASIN": p.get("parentAsin", ""),
                "Brand":       p.get("brand", ""),
                "Title":       p.get("title", ""),
                "Category":    cat_path,
                "BSR":         bsr,
                "Price":       price,
                "Category_IDs": ",".join(str(c) for c in all_cat_ids),
            })

    progress_bar.progress(1.0)
    status_text.text(f"Done - {len(rows)} products fetched.")
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Keepa Brand Exporter · Pangaea",
        page_icon="📦",
        layout="centered",
    )

    st.markdown("""
        <div style="display:flex; justify-content:space-between; align-items:center;
                    border-bottom:3px solid #1A1A2E; padding-bottom:10px; margin-bottom:20px">
            <div>
                <h2 style="margin:0; color:#1A1A2E">📦 Keepa Brand Exporter</h2>
                <p style="margin:0; color:#888; font-size:13px">
                    Fetch all brand ASINs via Keepa API → download CSV for Lead Gen Pipeline
                </p>
            </div>
            <div style="font-size:12px; font-weight:700; color:#457B9D; text-align:right">
                PANGAEA COMMERCE<br>
                <span style="font-weight:400; color:#aaa">eCommerce Agency</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    api_key = get_api_key()
    if not api_key:
        api_key = st.text_input(
            "Keepa API Key",
            type="password",
            placeholder="Paste your Keepa API key (or set KEEPA_API_KEY env var / secret)",
            help="Your key is never stored. For permanent use, set it as a Streamlit secret.",
        )

    if not api_key:
        st.info("Enter your Keepa API key above to get started.")
        st.stop()

    col1, col2 = st.columns([2, 1])
    with col1:
        brand = st.text_input(
            "Brand Name",
            placeholder='e.g. "Philips" — exactly as it appears on Amazon',
            help="Case-insensitive. Use the brand name as Keepa/Amazon displays it.",
        ).strip()
    with col2:
        marketplace_label = st.selectbox("Marketplace", list(MARKETPLACES.keys()), index=0)

    col3, col4 = st.columns([1, 1])
    with col3:
        bsr_limit = st.number_input(
            "Max BSR (Sales Rank)",
            min_value=1_000,
            max_value=1_000_000,
            value=200_000,
            step=10_000,
            help="Only include products ranked better than this.",
        )
    with col4:
        max_asins = st.number_input(
            "Max ASINs cap",
            min_value=100,
            max_value=MAX_ASINS,
            value=2000,
            step=100,
            help="Safety cap to avoid unexpected token costs on very large brands.",
        )

    domain = MARKETPLACES[marketplace_label]

    if st.button("Check remaining tokens"):
        try:
            api = get_keepa_api(api_key)
            tokens = api.tokens_left
            st.info(f"🪙 Keepa tokens remaining: **{tokens:,}**")
        except Exception as e:
            st.error(f"API error: {e}")

    st.divider()

    if not brand:
        st.warning("Enter a brand name to continue.")
        st.stop()

    if st.button(f"🔍 Fetch '{brand}' from {marketplace_label}", type="primary"):
        with st.spinner("Connecting to Keepa API..."):
            try:
                api = get_keepa_api(api_key)
            except Exception as e:
                st.error(f"Could not connect to Keepa API: {e}")
                st.stop()

        st.markdown("**Step 1 — Finding ASINs...**")
        finder_bar  = st.progress(0.0)
        finder_text = st.empty()
        finder_text.text("Running Product Finder...")

        try:
            asins = fetch_asins_for_brand(api, brand, int(bsr_limit), domain)
        except Exception as e:
            st.error(f"Product Finder error: {e}")
            st.stop()

        finder_bar.progress(1.0)

        if not asins:
            st.warning(
                f"No ASINs found for brand **'{brand}'** with BSR <= {bsr_limit:,} "
                f"on {marketplace_label}. "
                "Try adjusting the brand name or increasing the BSR limit."
            )
            st.stop()

        if len(asins) > max_asins:
            st.warning(
                f"Found **{len(asins):,}** ASINs — capping at **{max_asins:,}** "
                f"to control token cost."
            )
            asins = asins[:max_asins]

        finder_text.text(f"✅ Found {len(asins):,} ASINs")
        est_tokens = len(asins) + 50
        st.caption(f"Estimated token cost: ~{est_tokens:,} tokens")

        st.markdown("**Step 2 — Fetching product details...**")
        detail_bar  = st.progress(0.0)
        detail_text = st.empty()

        try:
            df = fetch_product_details(api, asins, domain, detail_bar, detail_text)
        except Exception as e:
            st.error(f"Product query error: {e}")
            st.stop()

        if df.empty:
            st.error("No product data returned. Check API key permissions.")
            st.stop()

        st.success(f"✅ Exported **{len(df):,} ASINs** for brand **{brand}**")

        st.markdown("**Preview (first 20 rows)**")
        st.dataframe(
            df[["ASIN", "Parent_ASIN", "Brand", "Title", "BSR", "Price", "Category"]]
            .head(20),
            use_container_width=True,
        )

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Total ASINs",    f"{len(df):,}")
        col_b.metric("Unique Parents", f"{df['Parent_ASIN'].nunique():,}")
        col_c.metric("Avg Price",
                     f"€{df['Price'].dropna().mean():.2f}" if not df['Price'].dropna().empty else "n/a")

        if df["Category"].notna().any():
            st.markdown("**Top Categories (by ASIN count)**")
            df["_leaf"] = df["Category"].apply(
                lambda x: x.split(" / ")[-1].strip() if isinstance(x, str) and x else "Unknown"
            )
            top_cats = df["_leaf"].value_counts().head(10).reset_index()
            top_cats.columns = ["Category", "ASIN Count"]
            st.dataframe(top_cats, use_container_width=True, hide_index=True)
            df.drop(columns=["_leaf"], inplace=True)

        ts       = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"keepa_{brand.lower().replace(' ', '_')}_{marketplace_label[:2].lower()}_{ts}.csv"

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        st.download_button(
            label="⬇️ Download CSV (for Lead Gen Pipeline)",
            data=csv_buffer.getvalue().encode("utf-8"),
            file_name=filename,
            mime="text/csv",
            type="primary",
        )

        st.caption(
            "Place this CSV anywhere and pass it via --keepa path/to/file.csv "
            "when running pipeline.py."
        )

        try:
            tokens_left = api.tokens_left
            st.info(f"🪙 Keepa tokens remaining after export: **{tokens_left:,}**")
        except Exception:
            pass


if __name__ == "__main__":
    main()
