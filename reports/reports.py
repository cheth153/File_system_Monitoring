import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os
import time
from datetime import datetime
DB_PATH = r"C:\Users\Admin\Database\new_db.db"


REPORT_PATH=r"C:\Users\Admin\OneDrive\Desktop\Python\learning\file_sys_project\reports\tables"
FIG_PATH=r"C:\Users\Admin\OneDrive\Desktop\Python\learning\file_sys_project\reports\figures"

if not os.path.exists(REPORT_PATH):
    os.makedirs(REPORT_PATH)

LAST_DB_PATH = os.path.join(REPORT_PATH, "last_snapshot.csv")

ts=datetime.now().strftime("%H%M%S")   
def gen_reports():
    with sqlite3.connect(DB_PATH) as conn:
        df=pd.read_sql_query("SELECT * FROM new_db",conn)

    #CSV reports
    # 
    df['f_type'].value_counts().to_csv(os.path.join(REPORT_PATH,f"file_types_{ts}.csv"))
    df.sort_values(by='f_size_Kb',ascending=False).head(10).to_csv(os.path.join(REPORT_PATH,f"Large_files_{ts}.csv"))


    #Graphs
    file_counts=df['f_type'].value_counts()
    types=file_counts.index
    count=file_counts.values
    plt.pie(count, labels=types, autopct='%1.1f%%',shadow=False, startangle=90)
    plt.title("File Type Distribution")
    plt.savefig(FIG_PATH, dpi=100, bbox_inches="tight")

    top_files = df.sort_values(by="f_size_Kb", ascending=False).head(10)
    
    if os.path.exists(LAST_DB_PATH):
        prev_df = pd.read_csv(LAST_DB_PATH)

        current_files = set(df['path'])
        previous_files = set(prev_df['path'])

        added = current_files - previous_files
        deleted = previous_files - current_files

        if added:
            df[df['path'].isin(added)].to_csv(
                os.path.join(REPORT_PATH, f"added_files_{ts}.csv"), index=False
            )
            print(f" Reported {len(added)} new file(s).")

        if deleted:
            prev_df[prev_df['path'].isin(deleted)].to_csv(
                os.path.join(REPORT_PATH, f"deleted_files_{ts}.csv"), index=False
            ) 

    df.to_csv(LAST_DB_PATH, index=False)        



if __name__ == "__main__":
    gen_reports()