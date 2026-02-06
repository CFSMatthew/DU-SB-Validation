import pandas as pd
import argparse
import os


def read_excel_all_sheets(path, **kwargs):
    xls = pd.ExcelFile(path)
    sheet_names = xls.sheet_names

    if len(sheet_names) == 1:
        return pd.read_excel(path, **kwargs)

    dfs = []
    for sheet in sheet_names:
        df = pd.read_excel(path, sheet_name=sheet, **kwargs)
        df["__source_sheet"] = sheet  # optional
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def compare_DU_tasks(du_forecast, uac_forecast):
    du_temp_df = du_forecast.copy()
    uac_temp_df = uac_forecast.copy()
    uac_temp_df["Task"] = uac_temp_df["Task"].str.replace(r"_#\d+", "", regex=True)

    for index, row in du_temp_df.iterrows():
        du_task_name = str(row["Uproc"])
        matches = uac_temp_df["Task"].astype(str).str.contains(
            du_task_name, case=False, na=False
        )
        if matches.any():
            du_temp_df = du_temp_df.drop(index=index)

    du_temp_df = du_temp_df[~du_temp_df["Uproc"].str.startswith(("H_", "T_"), na=False)]
    return du_temp_df


def compare_UAC_tasks(uac_forecast, du_forecast):
    uac_temp_df = uac_forecast.copy()
    du_temp_df = du_forecast.copy()

    uac_temp_df["Task"] = uac_temp_df["Task"].str.replace(r"_#\d+", "", regex=True)

    for index, row in uac_temp_df.iterrows():
        uac_task_name = str(row["Task"])
        task_type = str(row.get("Task Type", ""))

        if task_type.lower() == "workflow":
            stripped_task_name = "_".join(uac_task_name.split("_")[:-1])

            matches_uproc = du_temp_df["Uproc"].astype(str).str.contains(
                stripped_task_name, case=False, na=False
            )
            matches_session = du_temp_df["Session"].astype(str).str.contains(
                stripped_task_name, case=False, na=False
            )

            if matches_uproc.any() or matches_session.any():
                uac_temp_df = uac_temp_df.drop(index=index)
        else:
            matches = du_temp_df["Uproc"].astype(str).str.contains(
                uac_task_name, case=False, na=False
            )
            if matches.any():
                uac_temp_df = uac_temp_df.drop(index=index)

    uac_temp_df = uac_temp_df[uac_temp_df["Run/Skip Evaluation"] != "Skip"]
    uac_temp_df["Task"] = uac_temp_df["Task"].str.replace("_recurring", "")
    return uac_temp_df


# ===================== DEFAULT MODE =====================

def run_default_mode():
    print("Running DEFAULT validation mode...")

    du_forecast = read_excel_all_sheets("History (DU)\Job_History(Feb 02).xlsx")
    uac_forecast = read_excel_all_sheets(
        "Forecasts (UAC)\ops_trigger_forecast Jan_Sat31st-Feb_Monday02nd.xlsx"
    )
    
    # 31/01/2026 23:59:55
    # 1/2/2026 12:00:28 AM
    
    #du_forecast["Start Date"] = (du_forecast["Start Date"].astype(str).str.replace("r\s?(AM|PM)$", "", regex=True))
    #du_forecast["Start Date"] = du_forecast["Start Date"].astype(str).str.replace("PM", "", regex=False)

    du_forecast["Start Date"] = pd.to_datetime(
        du_forecast["Start Date"],
        format="%d/%m/%Y %H:%M:%S",
        #format="%Y/%d/%m %H:%M:%S",
        errors="coerce"
    )
    du_forecast = du_forecast[du_forecast["Start Date"].dt.month == 2]
    # print(du_forecast.query("Uproc == 'T_DNF_SKU_SRC_NETWK_GRP'")['Start Date'])
    # print(du_forecast.size)
    uac_forecast["Launch Time"] = pd.to_datetime(
        uac_forecast["Launch Time"],
        format="%Y-%m-%d %H:%M:%S %z",
        errors="coerce"
    ).dt.tz_localize(None)

    uac_forecast = uac_forecast[uac_forecast["Launch Time"].dt.day == 2]

    du_comparisons = compare_DU_tasks(du_forecast, uac_forecast)
    uac_comparisons = compare_UAC_tasks(uac_forecast, du_forecast)

    with pd.ExcelWriter("Validation_Report.xlsx") as writer:
        du_forecast.to_excel(writer, sheet_name="du_history_Mon_2nd", index=False)
        uac_forecast.to_excel(writer, sheet_name="uac_forecast_Mon_2nd", index=False)
        du_comparisons.to_excel(writer, sheet_name="du_comparisons", index=False)
        uac_comparisons.to_excel(writer, sheet_name="uac_comparisons", index=False)

    print("Validation_Report.xlsx created successfully.")


def run_mismatch_mode():
    print("Running MISMATCH overview mode...")

    mismatch_dir = "Reports/Mismatch Reports"

    if not os.path.exists(mismatch_dir):
        print(f"Directory not found: {mismatch_dir}")
        return

    files = [f for f in os.listdir(mismatch_dir) if f.lower().endswith((".xlsx", ".xls"))]

    if not files:
        print("No Excel files found in Mismatch Overview.")
        return

    print(f"Found {len(files)} files. Processing...\n")

    all_dfs = []  # 👈 store each file's dataframe here

    for file in files:
        full_path = os.path.join(mismatch_dir, file)
        print(f"Processing: {file}")

        df = pd.read_excel(full_path, sheet_name="du_comparisons")

        df["__source_file"] = file  # optional but VERY useful for tracing
        all_dfs.append(df)

        print(f"Rows loaded: {len(df)}")

    # 🔗 Combine all files into one dataframe
    if not all_dfs:
        print("No data collected.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows before deduplication: {len(combined_df)}")

    # 🧠 Remove duplicates based on BOTH Uproc and Session
    combined_df_deduped = combined_df.drop_duplicates(subset=["Uproc", "Session", "Task"])
    print(f"Total rows after deduplication: {len(combined_df_deduped)}")
    combined_df_deduped["Start Date"] = pd.to_datetime(
        combined_df_deduped["Start Date"],
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce"
    )

    # 💾 Save result
    output_path = "Reports/Mismatch Reports/Mismatch_Report.xlsx"
    combined_df_deduped.to_excel(output_path, index=False)

    print(f"\nSaved deduplicated mismatch report to {output_path}")
        
    


# ===================== MAIN =====================

def main():
    parser = argparse.ArgumentParser(description="DU/UAC Validation & Mismatch Tool")
    parser.add_argument(
        "-M",
        "--mismatch",
        action="store_true",
        help="Run mismatch overview mode instead of default validation"
    )

    args = parser.parse_args()

    if args.mismatch:
        run_mismatch_mode()
    else:
        run_default_mode()


if __name__ == "__main__":
    main()
