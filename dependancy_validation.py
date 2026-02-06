import pandas as pd

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

def main():
    edge_df = read_excel_all_sheets("Dependancy\ops_task_workflow_edge20260115.xlsx")
    vertex_df = read_excel_all_sheets("Dependancy\ops_task_workflow_vertex20260115.xlsx")
    
    edge_df["WF-S"] = edge_df["Workflow"].astype(str) + "-" + edge_df["Source Vertex Id"].astype(str)
    edge_df["WF-T"] = edge_df["Workflow"].astype(str) + "-" + edge_df["Target Vertex Id"].astype(str)
    edge_df["Source"] = ""
    edge_df["Target"] = ""
    for index, row in edge_df.iterrows():
        source_id = row["Source Vertex Id"]
        source_workflow = row["Workflow"]
        source_task_name = (vertex_df.query("`Vertex Id` == @source_id and Workflow == @source_workflow")["Task"].iloc[0])
        edge_df.at[index, "Source"] = source_task_name
        
        target_id = row["Target Vertex Id"]
        target_task_name = (vertex_df.query("`Vertex Id` == @target_id and Workflow == @source_workflow")["Task"].iloc[0])
        edge_df.at[index, "Target"] = target_task_name

    #print(edge_df["Target"])
    output_path = r"Dependancy\dependency_report.xlsx"
    edge_df.to_excel(output_path, index=False)

    print(f"Saved report to {output_path}")

    
if __name__ == "__main__":
    main()