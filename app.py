import streamlit as st
import pandas as pd
import csv, os, zipfile, math, tempfile
st.set_page_config(page_title="Cross Join Shop × SKU", layout="centered")
st.title(":zap: Cross-Join Shop × SKU (Auto Split & Zip)")
st.write("Splits shop data dynamically based on SKU count (700,000 rule)")
# -------------------- File Upload --------------------
shop_file = st.file_uploader(
    "Upload Shop File (CSV / Excel)",
    type=["csv", "xlsx", "xls"]
)
sku_file = st.file_uploader(
    "Upload SKU File (CSV / Excel)",
    type=["csv", "xlsx", "xls"]
)
# -------------------- Load File --------------------
def load_file(uploaded_file):
    if uploaded_file.name.lower().endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)
# -------------------- Start Processing --------------------
if st.button(":rocket: Start Processing"):
    if not shop_file or not sku_file:
        st.warning("Please upload both Shop and SKU files.")
        st.stop()
    with st.spinner("Processing files..."):
        shop_df = load_file(shop_file)
        sku_df  = load_file(sku_file)
        total_skus = len(sku_df)
        shops_per_file = math.ceil(700000 / total_skus)
        shop_records = shop_df.to_dict("records")
        sku_records  = sku_df.to_dict("records")
        total_shops = len(shop_records)
        st.info(f":package: SKU Rows: {total_skus}")
        st.info(f":corner_shop: Shops per file: {shops_per_file}")
        st.info(f":file_folder: Total shops: {total_shops}")
        header = list(shop_df.columns) + [
            c for c in sku_df.columns if c not in shop_df.columns
        ]
        temp_dir = tempfile.mkdtemp()
        csv_files = []
        batch_size = 5000
        progress = st.progress(0)
        for part, start in enumerate(range(0, total_shops, shops_per_file), start=1):
            end = min(start + shops_per_file, total_shops)
            current_shops = shop_records[start:end]
            out_path = os.path.join(
                temp_dir,
                f"Output_Shop_SKU_Part{part}.csv"
            )
            csv_files.append(out_path)
            with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                buffer = []
                for shop in current_shops:
                    for sku in sku_records:
                        row = {}
                        for col in shop_df.columns:
                            row[col] = shop.get(col, "")
                        for col in sku_df.columns:
                            if col not in shop_df.columns:
                                row[col] = sku.get(col, "")
                        buffer.append(row)
                        if len(buffer) >= batch_size:
                            writer.writerows(buffer)
                            buffer.clear()
                if buffer:
                    writer.writerows(buffer)
            progress.progress(min(end / total_shops, 1.0))
        # -------------------- ZIP OUTPUT --------------------
        zip_path = os.path.join(temp_dir, "Output_Shop_SKU_All_Files.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in csv_files:
                zipf.write(file, os.path.basename(file))
        with open(zip_path, "rb") as f:
            st.success(":white_tick: Processing Completed!")
            st.download_button(
                label=":arrow_down: Download ZIP",
                data=f,
                file_name="Output_Shop_SKU_All_Files.zip",
                mime="application/zip"
            )
