# utils/mariadb_utils.py - Fixed version with proper flush
import os
import time
import random
import string
import subprocess
import pymysql

# ----- File size measurement -----
def get_ibd_allocated_bytes(datadir="/var/lib/mysql", db="flask_db", table="victimtable"):
    """Use ls -s like the paper does - measures actual allocated disk blocks"""
    path = f"{datadir}/{db}/{table}.ibd"
    try:
        output = subprocess.check_output(["ls", "-s", "--block-size=1", path])
        return int(output.split()[0])
    except Exception as e:
        print(f"[WARN] ls -s failed: {e}")
        return -1

# ----- DB Controller -----
class MariaDBController:
    def __init__(
        self,
        db: str,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        datadir: str = "/var/lib/mysql",
    ):
        self.db_name = db
        self.host = host or os.environ.get("DB_HOST", "mariadb_container")
        self.port = port or int(os.environ.get("DB_PORT", "3306"))
        self.user = user or os.environ.get("DB_USER", "root")
        self.password = password or os.environ.get("DB_PASSWORD", "your_root_password")
        self.datadir = datadir
        self.db_path = f"{datadir}/{db}/"
        self.old_edit_time = None

        # Connect
        self.conn = pymysql.connect(
            host=self.host, port=self.port,
            user=self.user, password=self.password,
            database=self.db_name, charset="utf8mb4",
            autocommit=True,
        )
        self.cur = self.conn.cursor()

    def _flush_and_wait_for_change(self, tablename):
        """Paper-style flush: FLUSH WITH READ LOCK + wait for mtime change"""
        ibd_path = self.db_path + tablename + ".ibd"
        if not os.path.exists(ibd_path):
            return

        if self.old_edit_time is None:
            self.old_edit_time = os.path.getmtime(ibd_path)

        self.cur.execute(f"FLUSH TABLES `{tablename}` WITH READ LOCK")

        # Wait for mtime to change (up to 3 seconds)
        for _ in range(30):
            if os.path.getmtime(ibd_path) != self.old_edit_time:
                break
            time.sleep(0.1)

        self.old_edit_time = os.path.getmtime(ibd_path)
        self.cur.execute("UNLOCK TABLES")

    def drop_table(self, tablename):
        self.cur.execute(f"DROP TABLE IF EXISTS `{tablename}`")

    def create_basic_table(self, tablename, varchar_len=500, compressed=True, encrypted=True):
        comp = "1" if compressed else "0"
        enc = "YES" if encrypted else "NO"
        self.cur.execute(f"DROP TABLE IF EXISTS `{tablename}`")
        sql = f"""
        CREATE TABLE `{tablename}` (
            id INT NOT NULL,
            data VARCHAR({varchar_len}),
            PRIMARY KEY(id)
        ) ENGINE=InnoDB PAGE_COMPRESSED={comp} ENCRYPTED={enc};
        """
        self.cur.execute(sql)
        time.sleep(1)
        # Initialize mtime
        ibd_path = self.db_path + tablename + ".ibd"
        if os.path.exists(ibd_path):
            self.old_edit_time = os.path.getmtime(ibd_path)

    def insert_row(self, tablename: str, idx: int, data: str):
        self.cur.execute(f"INSERT INTO `{tablename}` (id, data) VALUES (%s, %s)", (idx, data))
        self._flush_and_wait_for_change(tablename)

    def update_row(self, tablename: str, idx: int, data: str):
        self.cur.execute(f"UPDATE `{tablename}` SET data=%s WHERE id=%s", (data, idx))
        self._flush_and_wait_for_change(tablename)

    def delete_row(self, tablename: str, idx: int):
        self.cur.execute(f"DELETE FROM `{tablename}` WHERE id=%s", (idx,))
        self._flush_and_wait_for_change(tablename)

    def get_table_size(self, tablename):
        """Get allocated bytes using ls -s"""
        return get_ibd_allocated_bytes(self.datadir, self.db_name, tablename)

    def flush_and_wait(self, tablename, sleep_sec=0.2):
        self._flush_and_wait_for_change(tablename)

# ----- String generation -----
def get_filler_str(n):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return "".join(random.choices(alphabet, k=n))

def get_compressible_str(n, ch="a", char=None):
    c = ch if char is None else char
    return c * n
