import os
import pandas as pd
import json
import logging
import math
import re
import sqlglot
from sqlglot.expressions import Table, Column, CTE

def extract_all_blocks(main_content, code_format):
    """
    마크다운 형식의 코드 블록들을 모두 추출하는 파서 함수

    Args:
        main_content (str): 마크다운 형식의 코드 블록들이 포함된 문자열
        code_format (str): 추출할 코드 블록의 형식

    Returns:
        sql_blocks (list): 추출된 코드 블록들의 리스트
    """
    sql_blocks = []
    start = 0
    
    while True:

        sql_query_start = main_content.find(f"```{code_format}", start)
        if sql_query_start == -1:
            break
        

        sql_query_end = main_content.find("```", sql_query_start + len(f"```{code_format}"))
        if sql_query_end == -1:
            break 

        sql_block = main_content[sql_query_start + len(f"```{code_format}"):sql_query_end].strip()
        sql_blocks.append(sql_block)

        start = sql_query_end + len("```")
    
    return sql_blocks

def hard_cut(str_e, length=0):
    if length:
        if len(str_e) > length:
            str_e = str_e[:int(length)]+"\n"
    return str_e

def get_values_from_table(csv_data_str):
    return '\n'.join(csv_data_str.split('\n')[1:])

def search_file(directory, target_file):
    result = []
    for root, dirs, files in os.walk(directory):
        if target_file in files:
            result.append(os.path.join(root, target_file))
    return result

def get_longest(sql_list):
    sql_list_len = [len(i) for i in sql_list]
    sql_list_len_index = sql_list_len.index(max(sql_list_len))
    return sql_list[sql_list_len_index]

def get_shortest(sql_list):
    sql_list_len = [len(i) for i in sql_list]
    sql_list_len_index = sql_list_len.index(min(sql_list_len))
    return sql_list[sql_list_len_index]

import threading
def initialize_logger(log_path, logger_name=None):
    if logger_name is None:
        logger_name = threading.current_thread().name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    return logger

def extract_between(file_path, start_str, end_str):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        if not content:
            pass
    
    results = []
    start_index = 0

    while True:
        start_index = content.find(start_str, start_index)
        if start_index == -1:
            break
        start_index += len(start_str)
        end_index = content.find(end_str, start_index)
        if end_index == -1:
            break
        results.append(content[start_index:end_index])
        start_index = end_index + len(end_str)
    
    return results


def compare_pandas_table(pred, gold, condition_cols=[], ignore_order=False, tolerance=0.001):
    """_summary_

    Args:
        pred (Dataframe): _description_
        gold (Dataframe): _description_
        condition_cols (list, optional): _description_. Defaults to [].
        ignore_order (bool, optional): _description_. Defaults to True.

    """
    # print('condition_cols', condition_cols)

    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        if ignore_order_:
            v1, v2 = (sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))),
                    sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))))
        if len(v1) != len(v2):
            return False
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True
    
    if condition_cols != []:
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold
    pred_cols = pred

    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    score = 1
    for _, gold in enumerate(t_gold_list):
        if not any(vectors_match(gold, pred, ignore_order_=ignore_order) for pred in t_pred_list):
            score = 0
        else:
            for j, pred in enumerate(t_pred_list):
                if vectors_match(gold, pred, ignore_order_=ignore_order):
                    break

    return score

def clear_description(table_info):
    """
    테이블 정보에서 Description: 이후의 내용을 제거하는 함수

    Examples:
        - Input:
            '''
            컬럼명: user_id 타입: INTEGER Description: 사용자의 고유 식별자
            컬럼명: username 타입: VARCHAR Description: 사용자의 로그인명, 최대 50자
            컬럼명: email 타입: VARCHAR Description: 사용자의 이메일 주소, 유니크 제약조건
            '''

        - Output:
            '''
            컬럼명: user_id 타입: INTEGER 
            컬럼명: username 타입: VARCHAR 
            컬럼명: email 타입: VARCHAR 
            '''
    """
    return re.sub(r"Description:[^\n]*", "", table_info)

def get_table_info(test_path, sql_data, api, clear_des=False, full_tb_info=None):
    """
    테이블 정보를 가져오는 함수
    
    Args:
        test_path: "examples_snow" (예제들이 있는 기본 경로)
        sql_data: "sf_bq001" (처리할 인스턴스 ID)
        api: "snowflake" (데이터베이스 타입)
        clear_des: True (Description 제거 여부)
        full_tb_info: None 또는 캐시된 테이블 정보 딕셔너리
    
    Returns:
        table_info: GPT 프롬프트에 포함될 테이블 스키마 정보 (문자열)
    """
    if full_tb_info:
        return full_tb_info[sql_data]
    else:
        table_info_txt = ["prompts.txt"]
        table_info = ''
        for txt in table_info_txt:
            txt_path = search_file(os.path.join(test_path, sql_data), txt)
            for path in txt_path:
                with open(path, encoding="utf-8") as f:
                    table_info += f.read()
        if clear_des:
            if len(table_info) > 200000:
                table_info = clear_description(table_info)
        return table_info

def get_api_name(sql_data):
    if sql_data.startswith("sf"):
        return "snowflake"
    elif sql_data.startswith("fnf"):  # FNF 케이스 추가
        return "snowflake"
    elif sql_data.startswith("local"):
        return "sqlite"
    elif sql_data.startswith("bq") or sql_data.startswith("ga"):
        return "bigquery"
    else:
        raise NotImplementedError("Invalid file name.")

def remove_digits(s):
    '''
    테이블명에서 모든 숫자를 제거하는 함수

    Examples:
        - "users_2020"  → `remove_digits()` → "users_"
        - "users_2021"  → `remove_digits()` → "users_"
        - "orders_2020" → `remove_digits()` → "orders_"
    '''
    return re.sub(r'\d', '', s) # 모든 숫자를 제거

def is_file(filepath, suffix):
    return os.path.isfile(filepath) and filepath.lower().endswith(suffix)

def matching_at_same_position(s1, s2):
    min_length = min(len(s1), len(s2))
    matches = [s1[i] for i in range(min_length) if s1[i] == s2[i]]
    return "".join(matches)

def get_dictionary(db_path, task):
    """
    Output Examples:

        dictionaries = [
        'fnf001',
        'fnf002'
        ]

        task_dict = {
        'fnf001': 'What is the total number of orders placed by each customer?',
        'fnf002': 'What is the total revenue generated by each product?'
        }
        
    """
    json_path = os.path.join(db_path, f"spider2-{task}.jsonl")
    task_dict = {}
    with open(json_path, encoding="utf-8") as f:
        for line in f:
            line_js = json.loads(line)
            if task == "snow":
                task_dict[line_js['instance_id']] = line_js['instruction']
            elif task == "lite":
                task_dict[line_js['instance_id']] = line_js['question']
            elif task == "fnf":  # FNF 태스크 추가
                task_dict[line_js['instance_id']] = line_js['instruction']

    dictionaries = [entry for entry in os.listdir(db_path) if os.path.isdir(os.path.join(db_path, entry))]
    return dictionaries, task_dict

def get_db_id(db_path, ex_id):
    task = "lite"
    assert ex_id.startswith("local")
    json_path = os.path.join(db_path, f"spider2-{task}.jsonl")
    with open(json_path, encoding="utf-8") as f:
        for line in f:
            line_js = json.loads(line)
            if line_js['instance_id'] == ex_id:
                return line_js["db"]


def get_sqlite_path(db_path="", sql_data=None, db_id=None, task=None):
    if db_id:
        if task == "lite":
            local_db_pth = "../../spider2-lite/resource/databases/spider2-localdb"
            return os.path.join(local_db_pth, db_id+".sqlite")
        elif task == "BIRD":
            local_db_pth = "../../data/BIRD/dev_databases"
            return os.path.join(local_db_pth, db_id, db_id+".sqlite")
    if not db_path:
        return ""
    sql_data_path = os.path.join(db_path, sql_data)
    sqlite_path = None
    for sqlite in os.listdir(sql_data_path):
        if sqlite.endswith(".sqlite"):
            sqlite_path = os.path.join(sql_data_path, sqlite)
    return sqlite_path

def split_sql_safe(sql: str):
    """
    문자열에 여러 개의 SQL이 있을 경우, 이를 개별 SQL 문으로 분리하는 함수

    Examples:
        - Input:
            "SELECT * FROM users; SELECT * FROM orders;"
        - Output:
            ["SELECT * FROM users;", "SELECT * FROM orders;"]
    """
    statements = []
    current_stmt = []

    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            current_stmt.append(line)
        elif ";" in line:
            parts = line.split(";")
            for i, part in enumerate(parts):
                if i < len(parts) - 1:
                    current_stmt.append(part)
                    statements.append("\n".join(current_stmt).strip())
                    current_stmt = []
                else:
                    current_stmt.append(part)
        else:
            current_stmt.append(line)

    if current_stmt:
        final = "\n".join(current_stmt).strip()
        if final:
            statements.append(final)
    return statements

def clear_sample_rows(text, byte_limit=1000):
    """
    샘플 데이터를 제거하는 함수
    """
    pattern = re.compile(r"(Sample rows:\s*)(.*?)(\n-{10,}\n)", re.DOTALL)

    def trim_block(match):
        prefix = match.group(1)              # "Sample rows: "
        content = match.group(2).strip()     # 실제 샘플 데이터
        suffix = match.group(3)              # "\n----------\n"

        try:
            data = json.loads(content)
            # if isinstance(data, list):
            #     for row in data:
            #         for k, v in row.items():
            #             if isinstance(v, str):
            #                 v_bytes = v.encode("utf-8")
            #                 if len(v_bytes) > byte_limit:
            #                     print("Right json")
            #                     if digit_entropy_ratio(v_bytes[:byte_limit//10]) < 0.3:
            #                         row[k] = v_bytes[:1000].decode("utf-8", errors="ignore")
            #                     else:
            #                         row[k] = v_bytes[:byte_limit//10].decode("utf-8", errors="ignore")
            trimmed_json = json.dumps(data, ensure_ascii=False, indent=2)
            return prefix + trimmed_json + "\n" + suffix
        except Exception as e:
            # print("Err in sample rows", e)
            if digit_entropy_ratio(content[:byte_limit]) < 0.3:
                return prefix + content[:1000] + suffix
            return prefix + content[:byte_limit] + suffix

    return pattern.sub(trim_block, text)

def get_tb_info(text):
    tb = []
    for i in text.split("-"*50):
        i = i.strip()
        # 영어와 한글 모두 지원
        if i.startswith("Table full name:") or i.startswith("테이블 전체명:"):
            tb.append(i)
    return tb

def get_external(text):
    if "External knowledge that might be helpful: " in text:
        return text[text.find("External knowledge that might be helpful: "):text.find("The table structure information is")]
    else:
        return ""

def compute_precision_recall(predicted: set, ground_truth: set):
    if not predicted:
        precision = 0.0
    else:
        precision = len(predicted & ground_truth) / len(predicted)

    if not ground_truth:
        recall = 0.0
    else:
        recall = len(predicted & ground_truth) / len(ground_truth)

    return precision, recall

def digit_entropy_ratio(s: str) -> float:
    if not s:
        return 0.0
    s = s.replace(" ", "")
    digit_count = sum(c.isdigit() for c in s)
    return 1.0 - digit_count / len(s)

def extract_column_names(sql: str) -> list[str]:
    column_names = []
    for line in sql.splitlines():
        line = line.strip()
        if not line or line.upper().startswith("CREATE") or line.startswith(")") or line.startswith("PARTITION"):
            continue
        parts = line.split()
        if len(parts) >= 2:
            column_name = parts[0].strip('`",')
            column_names.append(column_name)
    return column_names

def is_valid_result(df_csv):
    df_csv = df_csv.fillna("")
    df_csv_str = df_csv.astype(str)
    nested_val = [(item) for i, row in enumerate(df_csv.values.tolist()) for j, item in enumerate(row) if isinstance(item, str) and '\n' in item in item]
    # print(df_csv_str)
    if nested_val:
        return False

    if ((df_csv_str == "0") | (df_csv_str == "")).all().any():
        return False

    return True

def filter_bijection_like_dict(d):
    keys = set(d.keys())
    new_d = {}

    for k, vs in d.items():
        filtered_values = [v for v in vs if v in keys]
        if filtered_values:
            new_d[k] = filtered_values

    return new_d

def is_csv_empty(path):
    try:
        df = pd.read_csv(path)
        return df.empty
    except pd.errors.EmptyDataError:
        return True

def extract_real_table_names(sql: str, dialect: str = "bigquery"):
    """
    SQL 쿼리를 보고, sqlglot 라이브러리를 사용하여 실제로 사용되는 테이블명과 컬럼명을 추출하는 함수 (CTE 제외)
    """
    expr = sqlglot.parse_one(sql, read=dialect)

    # CTE 이름 추출
    cte_names = {cte.alias for cte in expr.find_all(CTE)}
    cte_names_upper = {name.upper() for name in cte_names}

    # 실제 테이블명 추출 (CTE 제외)
    full_table_names = {
        table.sql(dialect=dialect)
        for table in expr.find_all(Table)
        if table.name.upper() not in cte_names_upper
    }

    # 테이블명, 컬럼명 추출
    return full_table_names, {column.name for column in expr.find_all(Column)}

def clear_name(table_names, do_remove_digits=True):
    """
    테이블명에서 불필요한 문자를 제거하는 함수
    """
    if isinstance(table_names, str):
        cleared = table_names.split(" ")[0].replace('"', '').replace("'", '').replace("`", '').replace("*", '').upper()
        if do_remove_digits:
            return remove_digits(cleared)
        else:
            return cleared
    if do_remove_digits:
        return {remove_digits(raw_name.split(" ")[0].replace('"', '').replace("'", '').replace("`", '').replace("*", '').upper()) for raw_name in table_names}
    return {raw_name.split(" ")[0].replace('"', '').replace("'", '').replace("`", '').replace("*", '').upper() for raw_name in table_names}

def remove_declare_lines(sql_script: str) -> str:
    lines = sql_script.splitlines()
    cleaned_lines = [line for line in lines if not line.strip().upper().startswith("DECLARE")]
    return "\n".join(cleaned_lines)

def clear_byte(rows):
    for i in rows:
        for k, v in i.items():
            if isinstance(v, str) and "bytearray(b" in v:
                i[k] = "bytearray(b'...')"
    return rows