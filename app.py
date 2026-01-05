import pandas as pd
import csv
import os
import zipfile
import threading
import queue
import streamlit as st

# Function to load the files (Excel or CSV)
def load_file(path):
    if path.endswith(".csv"):
        return pd.read_csv(path)
    return pd.read_excel(path)

# Worker that handles cross-join & file splitting
def worker(status_q, shop_file_path, sku_file_path, max_rows_per_file):
    try:
        shop_df = load_file(shop_file_path)
        sku_df = load_file(sku_file_path)

        output_folder = os.path.dirname(shop_file_path)
        batch_size = 5000
        part, current_rows = 1, 0
        csv_files = []

        # Preserve column order
        header = list(shop_df.columns) + [c for c in sku_df.columns if c not in shop_df.columns]

        sku_records = sku_df.to_dict('records')
        shop_records = shop_df.to_dict('records')
        total_shops = len(shop_records)

        # Initial output file
        out_path = os.path.join(output_folder, f"Output_Shop_SKU_Part{part}.csv")
        csv_files.append(out_path)
        f = open(out_path, 'w', newline='', encoding='utf-8-sig')
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()

        buffer = []
        for i, shop in enumerate(shop_records, start=1):

            for sku in sku_records:
                row = {}

                # Merge rows with column order preserved
                for col in shop_df.columns:
                    row[col] = shop.get(col, "")
                for col in sku_df.columns:
                    if col not in shop_df.columns:
                        row[col] = sku.get(col, "")

                buffer.append(row)
                current_rows += 1

                # Write in chunks
                if len(buffer) >= batch_size:
                    writer.writerows(buffer)
                    buffer.clear()

                # Split into new file when limit reached
                if current_rows >= max_rows_per_file:
                    if buffer:
                        writer.writerows(buffer)
                    buffer.clear()

                    f.close()
                    part += 1
                    out_path = os.path.join(output_folder, f"Output_Shop_SKU_Part{part}.csv")
                    csv_files.append(out_path)
                    f = open(out_path, 'w', newline='', encoding='utf-8-sig')
                    writer = csv.DictWriter(f, fieldnames=header)
                    writer.writeheader()
                    current_rows = 0

            # Update progress
            if i % 50 == 0 or i == total_shops:
                status_q.put((i, total_shops))

        if buffer:
            writer.writerows(buffer)
        f.close()

        # Create ZIP
        zip_path = os.path.join(output_folder, "Output_Shop_SKU_All_Files.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in csv_files:
                zipf.write(file, os.path.basename(file))

        for file in csv_files:
            os.remove(file)

        status_q.put(("done", zip_path))

    except Exception as e:
        status_q.put(("error", str(e)))

# Poll queue for updates
def poll_queue(status_q, progress_placeholder):
    try:
        while True:
            msg = status_q.get_nowait()

            if isinstance(msg, tuple) and msg[0] == "done":
                return ("done", msg[1])

            elif isinstance(msg, tuple) and msg[0] == "error":
                return ("error", msg[1])

            else:
                processed, total = msg
                percent = round(processed / total * 100, 2)
                progress_placeholder.text(f"Processed {processed}/{total} shops ({percent}%)...")

    except queue.Empty:
        return None


# Streamlit UI
def main():
    st.title("⚡  Master File (Streamlit Version)")

    shop_file = st.file_uploader("Upload Shop File (Excel/CSV)", type=["csv", "xlsx", "xls"])
    sku_file = st.file_uploader("Upload SKU File (Excel/CSV)", type=["csv", "xlsx", "xls"])

    # 👉 Input box for max rows per file
    max_rows_per_file = st.number_input(
        "Max rows per output CSV file:",
        min_value=10_000,
        max_value=5_000_000,
        value=900_000,
        step=10_000
    )

    if shop_file and sku_file:
        status_q = queue.Queue()

        if st.button("Start Process"):
            st.info("Starting processing...")
            progress_placeholder = st.empty()

            # Save uploaded files temporarily
            shop_file_path = shop_file.name
            sku_file_path = sku_file.name

            with open(shop_file_path, "wb") as f:
                f.write(shop_file.getbuffer())
            with open(sku_file_path, "wb") as f:
                f.write(sku_file.getbuffer())

            # Background processing thread
            t = threading.Thread(
                target=worker,
                args=(status_q, shop_file_path, sku_file_path, max_rows_per_file),
                daemon=True
            )
            t.start()

            # Update UI
            while t.is_alive():
                result = poll_queue(status_q, progress_placeholder)
                if result:
                    status, data = result
                    if status == "done":
                        st.success("Completed! Download your ZIP file below:")
                        with open(data, "rb") as f:
                            st.download_button(
                                "Download ZIP",
                                data=f,
                                file_name="Output_Shop_SKU_All_Files.zip",
                                mime="application/zip",
                            )
                        return
                    elif status == "error":
                        st.error(f"Error: {data}")
                        return


if __name__ == "__main__":
    main()
