import os
from datetime import datetime
import time
import sqlite3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import win32security

DB_PATH = r"C:\Users\Admin\Database\new_db.db"
WATCH_DIR = r"C:\Users\Admin\OneDrive\Desktop\personal doc"

def get_owner(file_path):
    try:
        sd = win32security.GetFileSecurity(file_path, win32security.OWNER_SECURITY_INFORMATION)
        owner_sid = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, owner_sid)
        return f"{domain}\\{name}"
    except Exception as e:
        print(f"[ACCESS DENIED] Could not get owner for {file_path}: {e}")
        return "Unknown"


def file_info(file_path):
    try:
        file_stat = os.stat(file_path)
        name, ext = os.path.splitext(os.path.basename(file_path))
        size_kb = file_stat.st_size // 1024
        mtime = datetime.fromtimestamp(file_stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        owner = get_owner(file_path)
        return name, ext, size_kb, mtime, owner, 0
    except FileNotFoundError:
        return None
    except PermissionError:
        return None
    except Exception as e:
        print(f"[ERROR] file_info failed for {file_path}: {e}")
        return None

def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS new_db (
                path TEXT PRIMARY KEY,
                f_name TEXT,
                f_type TEXT,
                f_size_Kb INTEGER,
                f_mtime TEXT,
                Owner TEXT,
                Modified_file_size INTEGER DEFAULT 0
            )
        """)
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Database initialization failed: {e}")
    finally:
        conn.close()

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            res = file_info(event.src_path)
            if res:
                print(f"[CREATED] {event.src_path} at {ts}")
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT OR REPLACE INTO new_db 
                        (path, f_name, f_type, f_size_Kb, f_mtime, Owner, Modified_file_size) 
                        VALUES (?,?,?,?,?,?,?)""",
                        (event.src_path, *res)
                    )
                    conn.commit()
        except Exception as e:
            print(f"[ERROR] on_created failed for {event.src_path}: {e}")

    def on_deleted(self, event):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[DELETED] {event.src_path} at {ts}")
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                if event.is_directory:
                    cursor.execute("DELETE FROM new_db WHERE path LIKE ?", (event.src_path+"%",))
                else:
                    cursor.execute("DELETE FROM new_db WHERE path = ?", (event.src_path,))
                conn.commit()
        except Exception as e:
            print(f"[ERROR] on_deleted failed for {event.src_path}: {e}")

    def on_modified(self, event):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            res = file_info(event.src_path)
            if res:
                print(f"[MODIFIED] {event.src_path} at {ts}")
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("UPDATE new_db SET Modified_file_size = ? WHERE path = ?",
                                   (res[2], event.src_path))
                    conn.commit()
        except Exception as e:
            print(f"[ERROR] on_modified failed for {event.src_path}: {e}")

    def on_moved(self, event):
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[MOVED] {event.src_path} -> {event.dest_path} at {ts}")
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM new_db WHERE path = ?", (event.src_path,))
                res = file_info(event.dest_path)
                if res:
                    cursor.execute("""
                        INSERT OR REPLACE INTO new_db 
                        (path, f_name, f_type, f_size_Kb, f_mtime, Owner, Modified_file_size) 
                        VALUES (?,?,?,?,?,?,?)""",
                        (event.dest_path, *res)
                    )
                    conn.commit()
        except Exception as e:
            print(f"[ERROR] on_moved failed for {event.src_path} -> {event.dest_path}: {e}")

def initial_scan():
    try:
        fs_paths = set()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for root, dirs, files in os.walk(WATCH_DIR):
            for f in files:
                full_path = os.path.join(root, f)
                res = file_info(full_path)
                if res:
                    fs_paths.add(full_path)
                    cursor.execute("""
                        INSERT OR REPLACE INTO new_db 
                        (path, f_name, f_type, f_size_Kb, f_mtime, Owner, Modified_file_size) 
                        VALUES (?,?,?,?,?,?,?)""",
                        (full_path, *res)
                    )
                    conn.commit()
        # Remove deleted files from DB
        cursor.execute("SELECT path FROM new_db")
        db_paths = set([row[0] for row in cursor.fetchall()])
        for removed in db_paths - fs_paths:
            cursor.execute("DELETE FROM new_db WHERE path = ?", (removed,))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] initial_scan failed: {e}")
    finally:
        conn.close()

def main():
    init_db()
    initial_scan()
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=True)
    observer.start()
    print(f"[INFO] Monitoring started on {WATCH_DIR}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[INFO] Stopping observer...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
