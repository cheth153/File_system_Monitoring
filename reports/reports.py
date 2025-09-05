import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime
import shutil

# ---------------- CONFIG ----------------
DB_PATH = r"C:\Users\Admin\Database\new_db.db"
REPORT_PATH = r"C:\Users\Admin\OneDrive\Desktop\Python\learning\file_sys_project\reports\tables"
FIG_PATH = r"C:\Users\Admin\OneDrive\Desktop\Python\learning\file_sys_project\reports\figures"
LAST_DB_PATH = os.path.join(REPORT_PATH, "last_snapshot.csv")
TREND_LOG = os.path.join(REPORT_PATH, "trend_log.csv")

# Ensure paths exist
os.makedirs(REPORT_PATH, exist_ok=True)
os.makedirs(FIG_PATH, exist_ok=True)

# ---------------- HELPERS ----------------
def load_db():
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query("SELECT * FROM new_db", conn)

def save_csv_reports(df):
    timestamp = datetime.now().strftime("%H:%M:%S")

    # File type distribution
    file_types_path = os.path.join(REPORT_PATH, "file_types.csv")
    file_types = df['f_type'].value_counts().reset_index()
    file_types.columns = ['f_type', 'count']
    file_types['generated_at'] = timestamp
    file_types.to_csv(file_types_path, index=False)

    # Top 10 largest files
    large_files_path = os.path.join(REPORT_PATH, "large_files.csv")
    largest = df.sort_values(by='f_size_Kb', ascending=False).head(10)
    largest['generated_at'] = timestamp
    largest.to_csv(large_files_path, index=False)

# ---------------- PLOTS ----------------
def plot_file_type_distribution(df):
    ts = datetime.now().strftime("%H:%M:%S")
    file_counts = df['f_type'].value_counts()

    plt.figure(figsize=(7, 7))
    wedges, texts, autotexts = plt.pie(
        file_counts.values,
        labels=file_counts.index,
        autopct=lambda p: f'{p:.1f}%' if p > 2 else '',  # show percentage only if >2%
        startangle=90,
        wedgeprops={'linewidth': 1, 'edgecolor': "white"}
    )

    # Add legend
    plt.legend(wedges, file_counts.index,
               title='File Types',
               loc='center left',
               bbox_to_anchor=(1, 0, 0.5, 2))

    # Add title with timestamp
    plt.title(f"File Type Distribution\nGenerated: {ts}", fontsize=14)
    plt.savefig(os.path.join(FIG_PATH, "file_type_distribution.png"), dpi=150)
    plt.close()


def plot_drive_usage(df):
    ts= datetime.now().strftime("%H:%M:%S")
    total, used, free = shutil.disk_usage("D:/")
    folder_size = df['f_size_Kb'].sum() * 1024  # KB → Bytes

    folder_gb = folder_size / (1024**3)
    other_gb = (used - folder_size) / (1024**3)
    free_gb = free / (1024**3)

    labels = ["This Folder", "Other Used", "Free Space"]
    sizes = [folder_gb, other_gb, free_gb]

    plt.figure(figsize=(8, 5))
    bars = plt.bar(labels, sizes)
    for bar, size in zip(bars, sizes):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                 f"{size:.2f} GB", ha="center", va="bottom")

    plt.ylabel("Size (GB)")
    plt.title(f"C Drive Usage Breakdown : {ts}")
    plt.savefig(os.path.join(FIG_PATH, "drive_usage.png"), dpi=150)
    plt.close()

def plot_memory_distribution(df):
    ts= datetime.now().strftime("%H:%M:%S")
    types_sizes = df.groupby('f_type')['f_size_Kb'].sum().sort_values(ascending=False)
    top_types = types_sizes[:5]
    others = pd.Series({'Others': types_sizes[5:].sum()})
    final_sizes = pd.concat([top_types, others])

    plt.figure(figsize=(7, 7))
    wedges, texts, autotexts = plt.pie(
        final_sizes,
        autopct=lambda p: f'{p:.1f}%' if p > 2 else '',
        startangle=90,
        wedgeprops={'linewidth': 1, 'edgecolor': "white"}
    )
    plt.legend(wedges, final_sizes.index, title='File Types',
               loc='center left', bbox_to_anchor=(1, 0, 0.5, 1))
    plt.title(f"Memory Distribution by File Type:{ts}", fontsize=14)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_PATH, "memory_distribution.png"), dpi=150)
    plt.close()

def plot_trend_log(trend_log):
    ts= datetime.now().strftime("%H:%M:%S")
    if not os.path.exists(trend_log):
        return

    trend_df = pd.read_csv(trend_log, parse_dates=['timestamp'])
    graph_df = trend_df.pivot_table(
        index='timestamp',
        columns='type',
        values=['added', 'deleted', 'modified'],
        aggfunc='sum',
        fill_value=0
    )

    plt.figure(figsize=(10, 6))
    for ftype in graph_df["added"].columns[:5]:
        plt.plot(graph_df.index, graph_df["added"][ftype], label=f"Added {ftype}", marker="o")
        plt.plot(graph_df.index, graph_df["deleted"][ftype], label=f"Deleted {ftype}", marker="x")
        plt.plot(graph_df.index, graph_df["modified"][ftype], label=f"Modified {ftype}", marker="s")

    plt.xlabel("Time")
    plt.ylabel("File Count")
    plt.title(f"File Type Trends Over Time : {ts}")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(FIG_PATH, "file_trend.png"), dpi=150)
    plt.close()

# ---------------- TREND TRACKING ----------------
def update_trend_log(df, prev_df):
    current_files = set(df['path'])
    previous_files = set(prev_df['path'])

    added = current_files - previous_files
    deleted = previous_files - current_files

    added_count = df[df['path'].isin(added)]['f_type'].value_counts().to_dict() if added else {}
    deleted_count = prev_df[prev_df['path'].isin(deleted)]['f_type'].value_counts().to_dict() if deleted else {}

    modified = []
    for path in current_files & previous_files:
        old_size = prev_df.loc[prev_df['path'] == path, 'f_size_Kb'].values[0]
        new_size = df.loc[df['path'] == path, 'f_size_Kb'].values[0]
        if old_size != new_size:
            modified.append(path)

    modified_count = df[df['path'].isin(modified)]['f_type'].value_counts().to_dict() if modified else {}
    all_types = set(added_count) | set(deleted_count) | set(modified_count)

    trend_rec = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for ftype in all_types:
        trend_rec.append({
            'timestamp': timestamp,
            'type': ftype,
            'added': added_count.get(ftype, 0),
            'deleted': deleted_count.get(ftype, 0),
            'modified': modified_count.get(ftype, 0)
        })

    if trend_rec:
        trend_df = pd.DataFrame(trend_rec)
        if os.path.exists(TREND_LOG):
            trend_df.to_csv(TREND_LOG, mode='a', header=False, index=False)
        else:
            trend_df.to_csv(TREND_LOG, index=False)

# ---------------- MAIN ----------------
def gen_reports():
    df = load_db()
    if df.empty:
        print("No data found in DB.")
        return

    # Save CSV reports
    save_csv_reports(df)

    # Generate plots
    plot_file_type_distribution(df)
    plot_drive_usage(df)
    plot_memory_distribution(df)

    # Update trend log if previous snapshot exists
    if os.path.exists(LAST_DB_PATH):
        prev_df = pd.read_csv(LAST_DB_PATH)
        update_trend_log(df, prev_df)

    # Save current snapshot for next run
    df.to_csv(LAST_DB_PATH, index=False)

    # Update trend plot
    plot_trend_log(TREND_LOG)

    print("Reports and graphs generated successfully.")

if __name__ == "__main__":
    gen_reports()
