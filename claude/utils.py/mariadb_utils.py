# utils/mariadb_utils.py
import os
import time
import random
import string
import pymysql  # PyMySQL로 통일

# ----- 파일시스템 크기 측정 유틸 -----
def _ibd_path(datadir, db, table):
    return f"{datadir}/{db}/{table}.ibd"

def get_ibd_sizes(datadir="/var/lib/mysql", db="flask_db", table="victimtable"):
    """
    st_size: 논리 파일 길이(바이트)
    st_blocks*512: 실제 할당된 바이트(스파스/홀펀칭 반영)  <-- 논문에서 쓰는 신호
    """
    p = _ibd_path(datadir, db, table)
    st = os.stat(p)
    logical = st.st_size
    allocated = st.st_blocks * 512  # 리눅스 블록 크기 512B
    return logical, allocated

def get_ibd_allocated_bytes(datadir="/var/lib/mysql", db="flask_db", table="victimtable"):
    return get_ibd_sizes(datadir, db, table)[1]

# ----- DB 컨트롤러 -----
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
        # 환경변수 우선(컨테이너에서 쉽게 쓰려고)
        self.db_name = db
        self.host = host or os.environ.get("DB_HOST", "mariadb_container")
        self.port = port or int(os.environ.get("DB_PORT", "3306"))
        self.user = user or os.environ.get("DB_USER", "root")
        self.password = password or os.environ.get("DB_PASSWORD", "your_root_password")
        self.datadir = datadir

        # 접속
        self.conn = pymysql.connect(
            host=self.host, port=self.port,
            user=self.user, password=self.password,
            database=self.db_name, charset="utf8mb4",
            autocommit=True,
        )
        self.cur = self.conn.cursor()

    # (논문은 파일크기 신호가 핵심이라 DDL/CRUD 최소화만 둠)
    def drop_table(self, tablename):
        self.cur.execute(f"DROP TABLE IF EXISTS `{tablename}`")

    def create_basic_table(self, tablename, varchar_len=500, compressed=True, encrypted=True):
        comp = "1" if compressed else "0"
        enc  = "YES" if encrypted else "NO"
        sql = f"""
        CREATE TABLE `{tablename}` (
            id   INT NOT NULL,
            data VARCHAR({varchar_len}),
            PRIMARY KEY(id)
        ) ENGINE=InnoDB
          ROW_FORMAT=DYNAMIC
          PAGE_COMPRESSED={comp}
          ENCRYPTED={enc};
        """
        self.cur.execute(sql)

    def insert_row(self, tablename: str, idx: int, data: str):
        self.cur.execute(f"INSERT INTO `{tablename}` (id, data) VALUES (%s, %s)", (idx, data))

    def update_row(self, tablename: str, idx: int, data: str):
        self.cur.execute(f"UPDATE `{tablename}` SET data=%s WHERE id=%s", (data, idx))

    def delete_row(self, tablename: str, idx: int):
        self.cur.execute(f"DELETE FROM `{tablename}` WHERE id=%s", (idx,))

    # 논리 크기(참고용): 노이즈가 커서 지표로 쓰지 말 것
    def get_table_size_logical(self, tablename):
        self.cur.execute("""
            SELECT DATA_LENGTH + INDEX_LENGTH
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
        """, (self.db_name, tablename))
        r = self.cur.fetchone()
        return r[0] if r else -1

    # 실제 신호(핵심): .ibd 할당 바이트
    def get_table_size_alloc(self, tablename):
        return get_ibd_allocated_bytes(self.datadir, self.db_name, tablename)

    # 플러시/대기: 파일시스템 반영 안정화
    def flush_and_wait(self, tablename, sleep_sec=0.2):
        self.cur.execute("FLUSH TABLES")
        # 커널 버퍼 반영 여유
        time.sleep(sleep_sec)

# ----- 문자열 생성 -----
def get_filler_str(n):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choices(alphabet, k=n))

def get_compressible_str(n, ch='a'):
    return ch * n

# ----- 간단 데모(논문식 신호 보기) -----
def demo_side_channel_compression():
    """
    논문식: .ibd '할당 바이트' 변화 관측
    전제:
      - 컨테이너에 mariadb_data 볼륨이 /var/lib/mysql 로 마운트되어 있어야 함(읽기전용 OK)
      - 테이블은 PAGE_COMPRESSED + ENCRYPTED
    """
    db_name = os.environ.get("DB_NAME", "flask_db")
    table   = "victimtable"

    c = MariaDBController(db=db_name)

    # 1) 초기화
    c.drop_table(table)
    c.create_basic_table(table, varchar_len=500, compressed=True, encrypted=True)

    # 2) 초기 크기
    c.flush_and_wait(table)
    logical0 = c.get_table_size_logical(table)
    alloc0   = c.get_table_size_alloc(table)
    print(f"[INIT] logical={logical0}B, allocated={alloc0}B")

    # 3) 페이지 경계 근처로 '필러' 밀어넣기 (아주 단순화)
    #    - 실제 논문 구현은 bootstrap + boundary 맞춤 + 반복 측정.
    #    - 여기서는 빠른 체감을 위해 1KB씩 증가시키며 경계 반응을 관찰.
    idx = 1
    step = 1024
    for i in range(20):
        payload = get_filler_str(step)  # 난수(잘 안 압축)
        c.insert_row(table, idx, payload); idx += 1
        c.flush_and_wait(table)

        logical = c.get_table_size_logical(table)
        alloc   = c.get_table_size_alloc(table)
        print(f"[FILL {i:02d}] +{step}B random → logical={logical}, allocated={alloc}")

    # 4) 압축 잘 되는 패턴 주입(‘a’ 반복) → shrink 신호가 더 쉽게 나타남
    for i in range(10):
        payload = get_compressible_str(4096, 'a')  # 고압축
        c.insert_row(table, idx, payload); idx += 1
        c.flush_and_wait(table)

        logical = c.get_table_size_logical(table)
        alloc   = c.get_table_size_alloc(table)
        print(f"[COMP {i:02d}] +4KB 'a' → logical={logical}, allocated={alloc}")

if __name__ == "__main__":
    demo_side_channel_compression()
