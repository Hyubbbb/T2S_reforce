import sqlite3
import io
import csv
from utils import hard_cut
from google.cloud import bigquery
from google.oauth2 import service_account
import snowflake.connector
import json
import pandas as pd
from multiprocessing import Process, Queue
import threading

class SqlEnv:
    """
    SQL 실행 환경을 관리하는 클래스
    - SQLite, Snowflake, BigQuery 등 다양한 데이터베이스 연결 및 쿼리 실행을 지원
    - 연결 풀링과 타임아웃 처리 기능 제공
    """
    
    def __init__(self):
        """
        SqlEnv 인스턴스 초기화
        - 데이터베이스 연결들을 저장할 딕셔너리 생성
        """
        self.conns = {}  # 데이터베이스 연결을 저장하는 딕셔너리 {경로/ID: 연결객체}


    def get_rows(self, cursor, max_len):
        """
        데이터베이스 쿼리 결과에서 실제 데이터 행들을 가져오는 함수
        
        1. 커서(cursor)란?
        - SQL 쿼리를 실행한 후 결과를 가리키는 포인터 같은 것
        - 마치 파일을 읽을 때 현재 위치를 가리키는 것처럼, DB 결과에서 현재 읽고 있는 위치를 표시
        - 예: SELECT * FROM users 쿼리 실행 후, 결과 테이블의 각 줄을 순서대로 읽어올 수 있게 해줌
        
        2. 행(row)이란?
        - 테이블의 한 줄, 즉 하나의 레코드를 의미
        - 예: 사용자 테이블에서 ("김철수", 25, "서울") 이런 식으로 한 사람의 정보가 담긴 한 줄
        
        3. 이 함수가 하는 일:
        - 쿼리 결과 테이블에서 한 줄씩 읽어와서 리스트로 모음
        - 단, 결과가 너무 크면(max_len 초과) 중간에 멈춤 (메모리 절약)
        
        Args:
            cursor: SQL 쿼리 실행 결과를 담고 있는 커서 객체
            max_len (int): 결과 데이터의 최대 문자열 길이 (예: 30000자)
            
        Returns:
            list: 데이터베이스에서 가져온 행들의 리스트
                 예: [("김철수", 25, "서울"), ("이영희", 30, "부산"), ...]
        """
        rows = []           # 가져온 행들을 저장할 빈 리스트
        current_len = 0     # 현재까지 가져온 데이터의 총 길이
        
        # 커서에서 한 행씩 읽어오기 (for문으로 한 줄씩 처리)
        for row in cursor:
            row_str = str(row)  # 행을 문자열로 변환해서 길이 측정
            rows.append(row)    # 행을 결과 리스트에 추가
            
            # 💡 메모리 보호: 데이터가 너무 많으면 중간에 멈춤
            # 예: 100만 개 행이 있어도 30,000자 분량만 가져오고 멈춤
            if current_len + len(row_str) > max_len:
                break
            current_len += len(row_str)
        
        return rows


    def get_csv(self, columns, rows):
        """
        컬럼과 행 데이터를 CSV 형식의 문자열로 변환
        
        Args:
            columns (list): 컬럼명 리스트
            rows (list): 행 데이터 리스트
            
        Returns:
            str: CSV 형식의 문자열
        """
        output = io.StringIO()  # 메모리 내 문자열 버퍼 생성
        writer = csv.writer(output)
        writer.writerow(columns)    # 헤더 행 작성
        writer.writerows(rows)      # 데이터 행들 작성
        csv_content = output.getvalue()
        output.close()
        return csv_content


    def start_db_sf(self, ex_id):
        """
        Snowflake 데이터베이스 연결 시작
        
        Args:
            ex_id (str): 예제 ID (연결 식별자로 사용)
        """
        # 이미 연결된 경우 새로 연결하지 않음
        if ex_id not in self.conns.keys():
            # snowflake_credential.json 파일에서 인증 정보 로드
            snowflake_credential = json.load(open("./snowflake_credential.json"))
            self.conns[ex_id] = snowflake.connector.connect(**snowflake_credential)


    def close_db(self):
        """
        모든 데이터베이스 연결 종료
        - 메모리 누수 방지를 위해 모든 연결을 안전하게 종료
        """
        # print("Close DB")
        for key, conn in list(self.conns.items()):
            try:
                if conn:
                    conn.close()
                    # print(f"Connection {key} closed.")
                    del self.conns[key]  # 연결 딕셔너리에서 제거
            except Exception as e:
                print(f"When closing DB for {key}: {e}")

            
    def exec_sql_sf(self, sql_query, save_path, max_len, ex_id):
        """
        Snowflake에서 SQL 쿼리 실행
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
            save_path (str): 결과를 저장할 파일 경로
            max_len (int): 결과 데이터의 최대 길이
            ex_id (str): 예제 ID (연결 식별자)
            
        Returns:
            str or int: 성공 시 CSV 데이터 또는 0, 실패 시 에러 메시지
        """
        with self.conns[ex_id].cursor() as cursor:
            try:
                cursor.execute(sql_query)
                column_info = cursor.description
                rows = self.get_rows(cursor, max_len)
                columns = [desc[0] for desc in column_info]
            except Exception as e:
                return "##ERROR##"+str(e)

        if not rows:
            return "No data found for the specified query.\n"
        else:
            csv_content = self.get_csv(columns, rows) # csv 형식으로 변환
            if save_path:
                # 파일로 저장
                with open(save_path, 'w', newline='', encoding='utf-8') as f:
                    f.write(csv_content)
                return 0
            else:
                # 문자열로 반환 (길이 제한 적용)
                return hard_cut(csv_content, max_len)


    def execute_sql_api(self, sql_query, ex_id, save_path=None, api="sqlite", max_len=30000, sqlite_path=None, timeout=300):
        """
        데이터베이스 종류에 따라 적절한 SQL 실행 함수 호출
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
            ex_id (str): 예제 ID
            save_path (str, optional): 결과를 저장할 파일 경로
            api (str): 데이터베이스 종류 ("sqlite", "snowflake", "bigquery")
            max_len (int): 결과 데이터의 최대 길이
            sqlite_path (str, optional): SQLite 파일 경로
            timeout (int): 타임아웃 시간 (초)
            
        Returns:
            str or dict: 성공 시 결과 데이터, 실패 시 에러 정보
        """
        if api == "bigquery":
            result = self.exec_sql_bq(sql_query, save_path, max_len)

        elif api == "snowflake":
            # 1. Snowflake 연결 확인
            if ex_id not in self.conns.keys():
                self.start_db_sf(ex_id)
            # 2. SQL 실행
            result = self.exec_sql_sf(sql_query, save_path, max_len, ex_id)
            
        elif api == "sqlite":
            # 1. SQLite 연결 확인
            if sqlite_path not in self.conns.keys():
                self.start_db_sqlite(sqlite_path)
            # 2. 타임아웃 처리와 함께 SQL 실행
            result = self.execute_sqlite_with_timeout(sql_query, save_path, max_len, sqlite_path, timeout=300)
            # result = self.exec_sql_sqlite(sql_query, save_path, max_len, sqlite_path)

        # 에러 처리
        if "##ERROR##" in str(result):
            return {"status": "error", "error_msg": str(result)}
        else:
            return str(result)


















                
    # 주석 처리된 스레딩 방식의 타임아웃 구현 (참고용)
    # def execute_sqlite_with_timeout(self, sql_query, save_path, max_len, sqlite_path, timeout=300):
    #     """
    #     SQLite 쿼리를 타임아웃과 함께 실행 (스레딩 사용)
    #     - 멀티프로세싱 방식보다 가벼우나 GIL 제한으로 인해 실제 타임아웃이 어려울 수 있음
    #     """
    #     result_holder = {"result": None}

    #     def target():
    #         try:
    #             result_holder["result"] = self.exec_sql_sqlite(sql_query, save_path, max_len, sqlite_path)
    #         except Exception as e:
    #             result_holder["result"] = {"status": "error", "error_msg": str(e)}

    #     thread = threading.Thread(target=target)
    #     thread.start()
    #     thread.join(timeout)

    #     if thread.is_alive():
    #         print(f"##ERROR## {sql_query} Timed out")
    #         return {"status": "error", "error_msg": f"##ERROR## {sql_query} Timed out\n"}
    #     else:
    #         return str(result_holder["result"])

    ######################### Snowflake 외 데이터베이스 관련 함수 #########################
    def start_db_sqlite(self, sqlite_path):
        """
        SQLite 데이터베이스 연결 시작
        
        Args:
            sqlite_path (str): SQLite 데이터베이스 파일 경로
        """
        # 이미 연결된 경우 새로 연결하지 않음
        if sqlite_path not in self.conns:
            # 읽기 전용 모드로 SQLite 연결
            uri = f"file:{sqlite_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
            self.conns[sqlite_path] = conn
            # print(f"sqlite_path: {sqlite_path}, (self.conns): {self.conns.keys()}")

    def exec_sql_sqlite(self, sql_query, save_path=None, max_len=30000, sqlite_path=None):
        """
        SQLite에서 SQL 쿼리 실행
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
            save_path (str, optional): 결과를 저장할 파일 경로
            max_len (int): 결과 데이터의 최대 길이
            sqlite_path (str): SQLite 데이터베이스 파일 경로
            
        Returns:
            str or int: 성공 시 CSV 데이터 또는 0, 실패 시 에러 메시지
        """
        cursor = self.conns[sqlite_path].cursor()
        try:
            cursor.execute(sql_query)
            column_info = cursor.description    # 컬럼 정보 가져오기
            rows = self.get_rows(cursor, max_len)
            columns = [desc[0] for desc in column_info]  # 컬럼명 추출
        except Exception as e:
            return "##ERROR##"+str(e)
        finally:
            try:
                cursor.close()
            except Exception as e:
                print("Failed to close cursor:", e)

        if not rows:
            return "No data found for the specified query.\n"
        else:
            csv_content = self.get_csv(columns, rows)
            if save_path:
                # 파일로 저장
                with open(save_path, 'w', newline='', encoding='utf-8') as f:
                    f.write(csv_content)
                return 0
            else:
                # 문자열로 반환 (길이 제한 적용)
                return hard_cut(csv_content, max_len)
            
    def exec_sql_bq(self, sql_query, save_path, max_len):
        """
        BigQuery에서 SQL 쿼리 실행
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
            save_path (str): 결과를 저장할 파일 경로
            max_len (int): 결과 데이터의 최대 길이
            
        Returns:
            str or int: 성공 시 CSV 데이터 또는 0, 실패 시 에러 메시지
        """
        # BigQuery 인증 정보 로드
        bigquery_credential = service_account.Credentials.from_service_account_file("./bigquery_credential.json")
        client = bigquery.Client(credentials=bigquery_credential, project=bigquery_credential.project_id)
        
        query_job = client.query(sql_query)
        try:
            result_iterator = query_job.result()
        except Exception as e:
            return "##ERROR##"+str(e)
            
        # 결과 행들을 딕셔너리 형태로 수집
        rows = []
        current_len = 0
        for row in result_iterator:
            if current_len > max_len:
                break
            current_len += len(str(dict(row)))
            rows.append(dict(row))
            
        df = pd.DataFrame(rows)
        
        # 결과가 비어있는지 확인
        if df.empty:
            return "No data found for the specified query.\n"
        else:
            # 저장 또는 반환
            if save_path:
                df.to_csv(f"{save_path}", index=False)
                return 0
            else:
                return hard_cut(df.to_csv(index=False), max_len)
            
    def execute_sqlite_with_timeout(self, sql_query, save_path, max_len, sqlite_path, timeout=300):
        """
        SQLite 쿼리를 타임아웃과 함께 실행 (멀티프로세싱 사용)
        - 오래 걸리는 쿼리가 무한정 실행되는 것을 방지
        
        Args:
            sql_query (str): 실행할 SQL 쿼리
            save_path (str): 결과를 저장할 파일 경로
            max_len (int): 결과 데이터의 최대 길이
            sqlite_path (str): SQLite 파일 경로
            timeout (int): 타임아웃 시간 (초, 기본값: 300초)
            
        Returns:
            str or dict: 성공 시 결과 데이터, 타임아웃 시 에러 정보
        """
        def target(q):
            """
            별도 프로세스에서 실행될 함수
            - SQL 실행 결과를 큐에 저장
            """
            result = self.exec_sql_sqlite(sql_query, save_path, max_len, sqlite_path)
            q.put(str(result))
            
        q = Queue()  # 프로세스 간 통신을 위한 큐
        p = Process(target=target, args=(q,))
        p.start()

        # 지정된 시간만큼 프로세스 완료 대기
        p.join(timeout)
        
        if p.is_alive():
            # 타임아웃 발생 시 프로세스 강제 종료
            try:
                p.terminate()
                p.join(timeout=2)
                if p.is_alive():
                    print("Terminate failed, forcing kill.")
                    p.kill()
                    p.join()
            except Exception as e:
                print(f"Error when stopping process: {e}")
            print(f"##ERROR## {sql_query} Timed out")
            return {"status": "error", "error_msg": f"##ERROR## {sql_query} Timed out\n"}
        else:
            # 정상 완료 시 결과 반환
            if not q.empty():
                result = q.get()
                return result
            else:
                raise RuntimeError("Process p dead")