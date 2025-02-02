import mariadb
import random
import string
import subprocess
import os
import time

class MariaDBController:
    def __init__(self, db: str, host: str = "127.0.0.1", port: int = 3307, root_password: str = "your_root_password", container_name: str = "mariadb_container"):
        self.db_name = db
        self.host = host
        self.port = port
        self.root_password = root_password
        self.container_name = container_name
        try:
            self.conn = mariadb.connect(
                user="root",
                password=self.root_password,
                host=self.host,
                port=self.port,
                database=self.db_name
            )
            self.cur = self.conn.cursor()
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")
            self.conn = None
            self.cur = None
        self.backupdict = dict()

    def drop_table(self, tablename):
        if self.cur:
            try:
                self.cur.execute(f"DROP TABLE IF EXISTS {tablename}")
                self.conn.commit()
            except mariadb.Error as e:
                print(f"Error dropping table {tablename}: {e}")

    def create_basic_table(self, tablename, varchar_len=100, compressed=False, encrypted=False):
        if self.cur:
            compressed_str = "1" if compressed else "0"
            encrypted_str = "YES" if encrypted else "NO"
            try:
                self.cur.execute(f"""
                    CREATE TABLE {tablename} (
                        id INT NOT NULL,
                        data VARCHAR({varchar_len}),
                        PRIMARY KEY(id)
                    ) ENGINE=InnoDB
                    PAGE_COMPRESSED={compressed_str}
                    ENCRYPTED={encrypted_str}
                """)
                self.conn.commit()
                print(f"Table {tablename} created with PAGE_COMPRESSED={compressed_str}, ENCRYPTED={encrypted_str}")
            except mariadb.Error as e:
                print(f"Error creating table {tablename}: {e}")

    def get_table_size(self, tablename, verbose=False):
        if self.cur:
            try:
                query = f"""
                    SELECT DATA_LENGTH + INDEX_LENGTH 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = '{self.db_name}' 
                      AND TABLE_NAME = '{tablename}';
                """
                self.cur.execute(query)
                result = self.cur.fetchone()
                if result:
                    table_size = result[0]
                    if verbose:
                        print(f"Size of table {tablename}: {table_size} bytes")
                    return table_size
                else:
                    print(f"Table {tablename} not found in information_schema.")
                    return -1
            except mariadb.Error as e:
                print(f"Error getting size for table {tablename}: {e}")
                return -1
        else:
            print("No active database connection.")
            return -1

    def insert_row(self, tablename: str, idx: int, data: str):
        if self.cur:
            try:
                self.cur.execute(f"INSERT INTO {tablename} (id, data) VALUES (%s, %s)", (idx, data))
                self.conn.commit()
                print(f"Inserted row {idx} into {tablename}.")
            except mariadb.Error as e:
                print(f"Error inserting row {idx} into {tablename}: {e}")

    def update_row(self, tablename: str, idx: int, data: str):
        if self.cur:
            try:
                self.cur.execute(f"UPDATE {tablename} SET data=%s WHERE id=%s", (data, idx))
                self.conn.commit()
                print(f"Updated row {idx} in {tablename}.")
            except mariadb.Error as e:
                print(f"Error updating row {idx} in {tablename}: {e}")

    def delete_row(self, tablename: str, idx: int):
        if self.cur:
            try:
                self.cur.execute(f"DELETE FROM {tablename} WHERE id={idx}")
                self.conn.commit()
                print(f"Deleted row {idx} from {tablename}.")
            except mariadb.Error as e:
                print(f"Error deleting row {idx} from {tablename}: {e}")

    def _stop_mariadb(self):
        try:
            subprocess.check_output(["docker", "stop", self.container_name])
            print(f"Docker container {self.container_name} stopped.")
        except subprocess.CalledProcessError as e:
            print(f"Error stopping Docker container {self.container_name}: {e}")

    def _start_mariadb(self):
        try:
            subprocess.check_output(["docker", "start", self.container_name])
            print(f"Docker container {self.container_name} started.")
            time.sleep(5)  # Wait for MariaDB to start
            self.conn = mariadb.connect(
                user="root",
                password=self.root_password,
                host=self.host,
                port=self.port,
                database=self.db_name
            )
            self.cur = self.conn.cursor()
            print(f"Reconnected to MariaDB in container {self.container_name}.")
        except mariadb.Error as e:
            print(f"Error reconnecting to MariaDB: {e}")
        except subprocess.CalledProcessError as e:
            print(f"Error starting Docker container {self.container_name}: {e}")

    # Backup and restore methods can remain the same if needed, but ensure paths are accessible.

def get_filler_str(data_len: int):
    return ''.join(
        random.choices(
            string.ascii_uppercase + string.ascii_lowercase + string.digits + string.punctuation,
            k=data_len
        )
    )

def get_compressible_str(data_len: int, char='a'):
    return char * data_len

def demo_side_channel_compression():
    """
    압축+암호화가 적용된 테이블을 만들고,
    반복적으로 압축 잘 되는 문자열 vs. 랜덤 문자열을 넣으며
    테이블의 크기 변화를 관찰.
    """
    db_name = "flask_db"  # 기존 설정에 맞춤
    tablename = "victimtable"

    # 1) MariaDBController 생성 (호스트 및 포트 설정 포함)
    controller = MariaDBController(
        db=db_name,
        host="127.0.0.1",
        port=3307,
        root_password="your_root_password",  # 실제 MariaDB root 비밀번호로 변경
        container_name="mariadb_container"  # Docker 컨테이너 이름
    )
    
    # 2) 기존 테이블 있으면 삭제
    controller.drop_table(tablename)
    
    # 3) 압축+암호화 테이블 생성 (VARCHAR(500) 예시)
    #    PAGE_COMPRESSED=1, ENCRYPTED=YES
    controller.create_basic_table(tablename, varchar_len=500, compressed=True, encrypted=True)
    
    # 4) 테이블 초기 사이즈 측정
    initial_size = controller.get_table_size(tablename, verbose=True)
    print(f"[INIT] Table={tablename}, size={initial_size} bytes")
    
    # 5) 반복 시나리오: 압축 문자열 vs 랜덤 문자열
    for i in range(1, 11):
        if i % 2 == 0:
            # 압축 잘 되는 문자열 (ex: 2000 'a' characters)
            data_str = get_compressible_str(2000, char='a')
            data_type = 'compressible'
        else:
            # 난잡한 문자열 (ex: 2000 random ASCII chars)
            data_str = get_filler_str(2000)
            data_type = 'random'
        
        # insert_row
        controller.insert_row(tablename, idx=i, data=data_str)

        # 테이블 사이즈 확인
        current_size = controller.get_table_size(tablename, verbose=True)
        print(f"[STEP {i}] Inserted type={data_type}, size={current_size} bytes")
    
    print("[DONE] Check the logs above for table size changes.")

if __name__ == "__main__":
    print("[*] Setting up DB and table...")
    demo_side_channel_compression()
