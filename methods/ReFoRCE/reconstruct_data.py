import os
import pandas as pd
from tqdm import tqdm
import argparse
import shutil
import sqlite3
from utils import remove_digits, is_file, clear_description, clear_sample_rows, extract_column_names, extract_real_table_names, get_api_name, clear_name, remove_declare_lines, clear_byte
import json
pd.set_option('display.max_colwidth', None)
THRESHOLD = 200000
WRONG_GOLD_TABLES = ["bq095", "bq350", "bq379", "bq396", "sf_bq084", "sf_bq200","sf_bq226", "sf_bq295", "sf_bq358"]
SKIP_GOLD_SQLS = ["bq350", "local039", "bq095", "bq374", "bq379", "bq396", "bq403", "bq406", "sf_bq233", "sf_bq273", "sf_local039", "sf_bq295"]
def process_ddl(ddl_file):
    """
    DDL 파일을 처리하여 테이블 대표 이름을 찾고, 중복된 테이블을 제거합니다.

    Args:
        ddl_file (pd.DataFrame): DDL 파일을 읽어온 데이터프레임

    Returns:
        ddl_file (pd.DataFrame): 중복된 테이블을 제거한 DDL 파일
        representatives (dict): 그룹별 대표 테이블들을 저장한 딕셔너리
            - Example:
                representatives = {
                    'users_': ['users_2020', 'users_2021', 'users_2022', ..., 'users_2023'],  # 15개
                    'orders_': ['orders_2020', 'orders_2021', 'orders_2022'],  # 3개
                    'products_': ['products_main']  # 1개
                }
    """
    table_names = ddl_file['table_name'].to_list() # e.g., ['users_2020', 'users_2021', 'orders_2020', 'orders_2021']
    representatives = {} # 그룹별 대표 테이블들을 저장할 딕셔너리

    # 1. 숫자 제거 후 그룹화
    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives.keys(): 
            representatives[remove_digits(table_names[i])] += [table_names[i]]
        else:
            representatives[remove_digits(table_names[i])] = [table_names[i]]

    # 2. 중복된 테이블 제거
    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives:
            if len(representatives[remove_digits(table_names[i])]) > 10: # 10개 이상의 테이블이 있으면 대표 테이블로 선정
                if ddl_file['table_name'][i] != representatives[remove_digits(table_names[i])][0]:
                    ddl_file = ddl_file.drop(index=i)
            else:
                # representatives[table_names[i]] = [table_names[i]]
                del representatives[remove_digits(table_names[i])] # 10개 미만의 테이블이 있으면 그룹 제거
    return ddl_file, representatives

def process_ddl_gold(ddl_file, gold_table_names, entry=None):
    table_names = ddl_file['table_name'].to_list()
    representatives = {}

    for i in range(len(ddl_file)):
        if not any(table_names[i].upper() in t for t in gold_table_names):
            ddl_file = ddl_file.drop(index=i)
    ddl_file.reset_index(drop=True, inplace=True)
    table_names = ddl_file['table_name'].to_list()

    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives.keys():
            representatives[remove_digits(table_names[i])] += [table_names[i]]
        else:
            representatives[remove_digits(table_names[i])] = [table_names[i]]

    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives:
            if len(representatives[remove_digits(table_names[i])]) > 10:
                if ddl_file['table_name'][i] != representatives[remove_digits(table_names[i])][0]:
                    ddl_file = ddl_file.drop(index=i)
            else:
                # representatives[table_names[i]] = [table_names[i]]
                del representatives[remove_digits(table_names[i])]

    return ddl_file, representatives

def process_ddl_gold_schema(ddl_file, full_table_names_with_omit, entry):
    table_names = ddl_file['table_name'].to_list()
    representatives = {}

    for i in range(len(ddl_file)):
        if not any(table_names[i].upper() in t for t in full_table_names_with_omit):
            if not any(remove_digits(table_names[i].upper()) in t for t in full_table_names_with_omit):
                ddl_file = ddl_file.drop(index=i)
    ddl_file.reset_index(drop=True, inplace=True)
    table_names = ddl_file['table_name'].to_list()

    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives.keys():
            representatives[remove_digits(table_names[i])] += [table_names[i]]
        else:
            representatives[remove_digits(table_names[i])] = [table_names[i]]

    for i in range(len(ddl_file)):
        if remove_digits(table_names[i]) in representatives:
            if len(representatives[remove_digits(table_names[i])]) > 10:
                if ddl_file['table_name'][i] != representatives[remove_digits(table_names[i])][0]:
                    ddl_file = ddl_file.drop(index=i)
            else:
                # representatives[table_names[i]] = [table_names[i]]
                del representatives[remove_digits(table_names[i])]

    return ddl_file, representatives

def check_table_names(ddl_path):
    ddl_file = pd.read_csv(ddl_path)
    temp_path = ddl_path.replace("DDL.csv", "DDL_tmp.csv")
    ddl_file['table_name'] = ddl_file['table_name'].str.split('.').str[-1]
    ddl_file.to_csv(temp_path, index=False)
    os.replace(temp_path, ddl_path)

# Step 1: (Optional) Raw data의 복잡한 폴더 구조를 정리
def make_folder(args):
    '''
    Raw data의 복잡한 폴더 구조를 정리

    - Before: examples_snow/sf_bq070/IDC/bigquery-public-data.idc_v18.dicom_all.json
                            ↓ Reconstruct ↓
    - After: examples_snow/sf_bq070/IDC/bigquery-public-data/idc_v18/dicom_all.json
    '''

    print("=" * 60)
    print("📁 STEP: 폴더 구조 정리 시작")
    print("=" * 60)
    
    example_folder = args.example_folder
    entries = os.listdir(example_folder)
    print(f"📂 대상 폴더: {example_folder}")
    print(f"📋 처리할 예제 수: {len(entries)}개")
    print("🔄 복잡한 파일명을 체계적인 폴더 구조로 변환 중...")
    print()
    
    for i, entry in enumerate(tqdm(entries, desc="폴더 구조 정리"), 1):
        entry1_path = os.path.join(example_folder, entry)
        if os.path.isdir(entry1_path):
            for project_name in os.listdir(entry1_path):
                project_name_path = os.path.join(entry1_path, project_name)
                if os.path.isdir(project_name_path):
                    for db_name in os.listdir(project_name_path):
                        db_name_path = os.path.join(project_name_path, db_name)
                        if db_name == "json":
                            os.remove(os.path.join(project_name_path, "json"))
                        elif (entry.startswith("sf") and db_name.endswith(".json")) or (entry.startswith("bq")) or (entry.startswith("ga")):
                            assert '.' in db_name.strip(".json")
                            folder_name = db_name.split(".")[0]
                            file_name = '.'.join(db_name.split(".")[1:])
                            folder_path = os.path.join(project_name_path, folder_name)
                            if not os.path.exists(folder_path):
                                os.mkdir(folder_path)
                            if entry.startswith("bq") or entry.startswith("ga"):
                                shutil.move(db_name_path, os.path.join(folder_path, file_name))
                            elif entry.startswith("sf"):
                                shutil.copy(db_name_path, os.path.join(folder_path, file_name))
                                os.remove(db_name_path)                                
                    if entry.startswith("sf") and "DDL.csv" in os.listdir(project_name_path):
                        ddl_path = os.path.join(project_name_path, "DDL.csv")
                        shutil.copy(ddl_path, os.path.join(folder_path, "DDL.csv"))
                        os.remove(ddl_path)
                    if entry.startswith("bq") or entry.startswith("ga"):
                        shutil.move(folder_path, os.path.join(entry1_path, folder_name))
                        shutil.rmtree(project_name_path)
    
    print("✅ 폴더 구조 정리 완료")
    print()

def compress_ddl(example_folder, add_description=False, add_sample_rows=False, rm_digits=False, schema_linked=False, clear_long_eg_des=False, sqlite_sl_path=None, reduce_col=False, use_gold_table=False, use_gold_schema=False):
    """데이터베이스 스키마와 메타데이터를 LLM이 읽을 수 있는 프롬프트로 변환합니다.
    
    이 함수는 Spider2 데이터셋 예제들을 처리하여 데이터베이스 스키마 정보, 테이블 메타데이터,
    그리고 외부 지식 문서들을 추출한 후 이를 결합하여 언어 모델이 SQL 생성 작업에 사용할 수 
    있는 구조화된 프롬프트를 생성합니다.
    
    Args:
        example_folder (str): 예제 인스턴스들이 포함된 폴더 경로 
            (예: "examples_snow").
        add_description (bool, optional): 출력 프롬프트에 컬럼 설명을 포함할지 여부. 
            기본값은 False.
        add_sample_rows (bool, optional): 각 테이블의 샘플 데이터 행을 포함할지 여부. 
            기본값은 False.
        rm_digits (bool, optional): 테이블명에서 숫자를 제거하고 유사한 테이블들을 
            그룹화할지 여부 (예: users_2021, users_2022 -> users). 기본값은 False.
        schema_linked (bool, optional): 스키마 링크된 DDL 파일을 사용할지 여부 
            (DDL.csv 대신 DDL_sl.csv). 기본값은 False.
        clear_long_eg_des (bool, optional): 프롬프트가 크기 임계값을 초과할 때 
            긴 설명을 제거할지 여부. 기본값은 False.
        sqlite_sl_path (str, optional): 로컬 데이터베이스용 SQLite 스키마 링킹 
            결과 JSON 파일 경로. 기본값은 None.
        reduce_col (bool, optional): 스키마 링킹 결과를 기반으로 컬럼을 줄일지 여부. 
            schema_linked=True가 필요함. 기본값은 False.
        use_gold_table (bool, optional): 골드 표준 테이블명을 사용해 테이블을 
            필터링할지 여부. 기본값은 False.
        use_gold_schema (bool, optional): 골드 표준 SQL 스키마를 사용해 테이블과 
            컬럼을 필터링할지 여부. 기본값은 False.
    
    Returns:
        None: 이 함수는 각 예제 폴더에 처리된 프롬프트를 "prompts.txt" 파일로 
        저장하지만 값을 반환하지는 않습니다.
    
    Raises(예외):
        FileNotFoundError: 필요한 DDL.csv 또는 JSON 메타데이터 파일이 없는 경우.
        json.JSONDecodeError: JSON 메타데이터 파일 형식이 잘못된 경우.
        pandas.errors.EmptyDataError: DDL.csv 파일이 비어있거나 손상된 경우.
    
    Note:
        - 생성된 프롬프트는 각 예제 디렉토리에 "prompts.txt"로 저장됩니다
        - 200KB 임계값을 초과하는 프롬프트는 자동으로 압축됩니다
        - .md 파일의 외부 지식이 있으면 자동으로 포함됩니다
        - 로컬 SQLite 데이터베이스의 경우 get_sqlite_data() 함수를 사용합니다
        - 테이블 구조 정보는 다음 형식으로 포맷됩니다: 
          {데이터베이스명: {스키마명: [테이블명들]}}
    
    Example:
        >>> compress_ddl(
        ...     example_folder="examples_snow",
        ...     add_description=True,
        ...     add_sample_rows=True,
        ...     rm_digits=True,
        ...     clear_long_eg_des=True
        ... )
        # examples_snow/ 내 모든 예제를 처리하고 prompts.txt 파일들을 생성합니다
    """
    print("=" * 60)
    print("🗄️  STEP: DDL 압축 및 프롬프트 생성 시작")
    print("=" * 60)
    
    entries = os.listdir(example_folder)
    print(f"📂 대상 폴더: {example_folder}")
    print(f"📋 처리할 예제 수: {len(entries)}개")
    print(f"⚙️  옵션 설정:")
    print(f"   - 설명 추가: {'✅' if add_description else '❌'}")
    print(f"   - 샘플 데이터 추가: {'✅' if add_sample_rows else '❌'}")
    print(f"   - 숫자 제거: {'✅' if rm_digits else '❌'}")
    print(f"   - 긴 설명 제거: {'✅' if clear_long_eg_des else '❌'}")
    print(f"   - 골드 테이블 사용: {'✅' if use_gold_table else '❌'}")
    print(f"   - 골드 스키마 사용: {'✅' if use_gold_schema else '❌'}")
    print()
    
    # 1. 각 example 폴더 순회
    for entry_idx, entry in enumerate(tqdm(entries, desc="예제 처리"), 1): 
        external_knowledge = None
        prompts = ''
        entry1_path = os.path.join(example_folder, entry)
        if os.path.isdir(entry1_path):
            print(f"  [{entry_idx:3d}/{len(entries)}] 🔄 {entry} 처리 중...")
            
            # 2. 각 예제별 골드 데이터 처리
            gold_table_names = None
            gold_column_names = None

            # 2-1. 골드 테이블 사용 시 
            if use_gold_table:
                for ex in gold:
                    if ex['instance_id'] == entry:
                        gold_table_names = set([i.upper() for i in ex["gold_tables"]])
                if gold_table_names is None or entry in WRONG_GOLD_TABLES:
                    shutil.rmtree(os.path.join(args.example_folder, entry))
                    print(f"      ⚠️  골드 테이블 누락으로 스킵: {entry}")
                    continue
                
            # 2-2. 골드 스키마 사용 시
            elif use_gold_schema:
                if entry in SKIP_GOLD_SQLS:
                    shutil.rmtree(os.path.join(args.example_folder, entry))
                    continue
                for ex in os.listdir(args.gold_sql_pth):
                    if ex.replace(".sql", "") == entry:
                        with open(os.path.join(args.gold_sql_pth, ex)) as f:
                            gold_sql = remove_declare_lines(f.read())
                        full_table_names_with_omit, gold_column_names = extract_real_table_names(gold_sql, get_api_name(ex))
                        gold_table_names = clear_name(full_table_names_with_omit, do_remove_digits=False)
                        gold_column_names = {i.upper() for i in gold_column_names}
                if gold_table_names is None:
                    shutil.rmtree(os.path.join(args.example_folder, entry))
                    print(f"      ⚠️  골드 스키마 누락으로 스킵: {entry}")
                    continue
                print(f"      ✅ 골드 스키마 로드 완료: {len(gold_table_names)}개 테이블")

            # 3. DB 스키마 처리
            # 3-1. 로컬 DB가 아닌 경우 (Snowflake, BigQuery, Google BigQuery)
            if not entry.startswith("local"):
                print(f"      🗄️  클라우드 DB 스키마 처리 시작...")
                table_dict = {}
                project_count = 0
                table_count = 0

                # 프로젝트 → 데이터베이스 → 스키마 순으로 순회
                # 프로젝트 순회
                for project_name in os.listdir(entry1_path):
                    if project_name == "spider":
                        continue
                    project_name_path = os.path.join(entry1_path, project_name)
                    if os.path.isdir(os.path.join(project_name_path)):
                        project_count += 1
                        print(f"        📊 프로젝트: {project_name}")
                        # 데이터베이스 순회
                        for db_name in os.listdir(project_name_path):
                            db_name_path = os.path.join(project_name_path, db_name)
                            assert os.path.isdir(db_name_path) == True and "DDL.csv" in os.listdir(db_name_path)

                            # 스키마 순회
                            for schema_name in os.listdir(db_name_path):
                                schema_name_path = os.path.join(db_name_path, schema_name)

                                # 스키마 파일 처리
                                if schema_name == "DDL.csv":
                                    representatives = None

                                    if entry.startswith("sf0"): # "sf0"가 뭘까? 
                                        check_table_names(schema_name_path)

                                    # 스키마 링크가 참인 경우, DDL 대신 DDL_sl.csv 파일 사용
                                    ddl_sl_flag = False
                                    if schema_linked:
                                        if os.path.exists(schema_name_path.replace("DDL.csv", "DDL_sl.csv")):
                                            ddl_sl_flag = True
                                            schema_name_path = schema_name_path.replace("DDL.csv", "DDL_sl.csv")
                                    ddl_file = pd.read_csv(schema_name_path)
                                    
                                    # clear ddl_file for sf
                                    # if entry.startswith("sf"):
                                    #     table_names_ = []
                                    #     for i in os.listdir(db_name_path):
                                    #         if i.endswith(".json"):
                                    #             table_names_ += [i.replace(".json", "").split(".")[-1]]
                                    #     ddl_file = ddl_file[ddl_file["table_name"].isin(table_names_)].reset_index(drop=True)
                                    #     assert not ddl_file.empty
                                    #     ddl_file.to_csv(schema_name_path, index=False)
                                    # print(ddl_file, entry)

                                    # 조건에 따른 DDL 처리 함수 호출
                                    if schema_linked and len(ddl_file['table_name'].to_list()) < 10:
                                        pass
                                    elif use_gold_table:
                                        ddl_file, representatives = process_ddl_gold(ddl_file, gold_table_names, entry)
                                    elif use_gold_schema:
                                        ddl_file, representatives = process_ddl_gold_schema(ddl_file, gold_table_names, entry)
                                    elif rm_digits:
                                        ddl_file, representatives = process_ddl(ddl_file)
                                    table_name_list = ddl_file['table_name'].to_list()
                                    ddl_file.reset_index(drop=True, inplace=True)
                                    print(f"          🗂️  데이터베이스: {db_name} ({len(table_name_list)}개 테이블)")

                                    # 3-1-1. 테이블별 상세 정보 처리
                                    for i in range(len(table_name_list)):
                                        # 3-1-1-1. 테이블 JSON 메타데이터 로드
                                        if os.path.exists(os.path.join(db_name_path, table_name_list[i]+".json")):           
                                            with open(os.path.join(db_name_path, table_name_list[i]+".json"), encoding="utf-8") as f:
                                                table_json = json.load(f)
                                        elif os.path.exists(os.path.join(db_name_path, db_name+'.'+table_name_list[i]+".json")):
                                            with open(os.path.join(db_name_path, db_name+'.'+table_name_list[i]+".json"), encoding="utf-8") as f:
                                                table_json = json.load(f)
                                        else:
                                            print(f"            ⚠️  테이블 메타데이터 누락: {table_name_list[i]}")
                                            continue

                                        table_count += 1
                                        # 3-1-1-2. 골드 테이블 사용 시
                                        if use_gold_table:
                                            if table_json["table_fullname"].upper() not in gold_table_names and not representatives:
                                                continue
                                        elif use_gold_schema:
                                            if table_json["table_fullname"].upper() not in gold_table_names and not representatives:
                                                continue
                                        
                                        # 3-1-1-3. 테이블 정보를 프롬프트에 추가
                                        prompts += "테이블 전체명: " + table_json["table_fullname"] + "\n"
                                        
                                        project_name_, db_name_, table_name_ = table_json["table_fullname"].split(".")
                                        table_dict.setdefault(project_name_, {}).setdefault(db_name_, [])


                                        if reduce_col and ddl_sl_flag:
                                            assert schema_linked
                                            full_name = table_json["table_fullname"]
                                            short_name = full_name.split(".")[-1].strip()

                                            ddl_file.columns = ddl_file.columns.str.strip().str.lower()
                                            ddl_file["table_name"] = ddl_file["table_name"].str.strip()
                                            matched = ddl_file[ddl_file["table_name"] == short_name].iloc[0]
                                            # assert len(matched) == 1, print(ddl_file["table_name"], short_name, entry)
                                            
                                            col_names = matched["ddl"]
                                        column_prefix = "column_"

                                        # 3-1-1-4. 컬럼 정보 추가
                                        for j in range(len(table_json[f"{column_prefix}names"])):
                                            table_des = ''
                                            if add_description: # --add_description 플래그 사용 시
                                                if j < len(table_json["description"]):
                                                    table_des = " 설명: " + str(table_json["description"][j]) if table_json["description"][j] else ""
                                                elif table_json[f"column_names"][j] != "_PARTITIONTIME":
                                                    print(f"{entry} 설명 불일치 {table_name_list[i]}")

                                            if reduce_col and ddl_sl_flag:
                                                if table_json[f"{column_prefix}names"][j] in col_names:
                                                    prompts += "컬럼명: " + table_json[f"{column_prefix}names"][j] + " 타입: " + table_json[f"{column_prefix}types"][j] + table_des +"\n"
                                            elif use_gold_schema:
                                                if table_json[f"{column_prefix}names"][j].upper() in gold_column_names:
                                                    prompts += "컬럼명: " + table_json[f"{column_prefix}names"][j] + " 타입: " + table_json[f"{column_prefix}types"][j] + table_des +"\n"
                                            else:
                                                prompts += "컬럼명: " + table_json[f"{column_prefix}names"][j] + " 타입: " + table_json[f"{column_prefix}types"][j] + table_des +"\n"
                                        
                                        # 3-1-1-5. 샘플 데이터 추가
                                        if add_sample_rows: # --add_sample_rows 플래그 사용 시
                                            if reduce_col and ddl_sl_flag:
                                                sample_rows = [{col: row[col] for col in extract_column_names(col_names) if col in row} for row in table_json["sample_rows"]]
                                            elif use_gold_schema:
                                                rows = []
                                                for row in table_json["sample_rows"]:
                                                    for col in gold_column_names:
                                                        for s in row.keys():
                                                            if col in s.upper():
                                                                rows.append({col: row[s]})
                                                if table_json["sample_rows"]:
                                                    assert rows, str(entry)+str(table_json) + str(gold_column_names)
                                                sample_rows = rows
                                            else:
                                                sample_rows = table_json["sample_rows"]
                                            sample_rows = clear_byte(sample_rows)
                                            prompts += "샘플 데이터:\n" + str(sample_rows) + "\n"
                                        table_dict[project_name_][db_name_] += [table_name_list[i]]
                                        if representatives is not None:
                                            if remove_digits(table_name_list[i]) in representatives:
                                                if len(representatives[remove_digits(table_name_list[i])]) > 1:
                                                    assert len(representatives[remove_digits(table_name_list[i])]) >= 10, representatives[remove_digits(table_name_list[i])]
                                                    prompts += f"유사한 구조를 가진 다른 테이블들: {representatives[remove_digits(table_name_list[i])]}\n"
                                                    table_dict[project_name_][db_name_] += representatives[remove_digits(table_name_list[i])]
                                        prompts += "\n" + "-" * 50 + "\n"
                                elif schema_name == "json":
                                    with open(schema_name_path, encoding="utf-8") as f:
                                        prompts += f.read()
                                        print(f.read())

                    # 3-1-2. .md 파일의 외부 지식이 있으면 자동으로 포함
                    elif is_file(project_name_path, "md"):
                        with open(project_name_path, encoding="utf-8") as f:
                            external_knowledge = f.read() # 외부 지식 파일 읽기
            
            # 3-2. 로컬 DB인 경우
            else:
                print(f"      💾 로컬 SQLite DB 처리 시작...")
                # 로컬 DB 파일 순회
                for sqlite in os.listdir(entry1_path):
                    if sqlite.endswith(".sqlite"):
                        sqlite_path = os.path.join(entry1_path, sqlite)
                        print(f"        📁 SQLite 파일: {sqlite}")
                if sqlite_sl_path:
                    with open(sqlite_sl_path, encoding="utf-8") as f:
                        sl_res = json.load(f)
                    for eg in sl_res:
                        if eg["instance_id"] == entry:
                            sl_info = eg
                            external_knowledge = "검색된 컬럼과 값들: " + str(sl_info['L_values']) if sl_info['L_values'] else ""
                table_names, prompts = get_sqlite_data(sqlite_path, entry, add_description=add_description, add_sample_rows=add_sample_rows, gold_table_names=gold_table_names, gold_column_names=gold_column_names)
            
            # 4. 프롬프트 저장
            print(f"      💾 프롬프트 생성 및 저장 중...")
            original_size = len(prompts)
            
            with open(os.path.join(entry1_path, "prompts.txt"), "w", encoding="utf-8") as f:
                # 4-1. 200KB 임계값을 초과하는 프롬프트는 자동으로 압축
                if len(prompts) > THRESHOLD:
                    print(f"        🗜️  프롬프트 크기 초과 ({original_size:,} bytes > {THRESHOLD:,} bytes), 샘플 데이터 압축 중...")
                    prompts = clear_sample_rows(prompts, byte_limit=10000)
                    print(f"        ✂️  샘플 데이터 압축 완료: {len(prompts):,} bytes")

                # 4-2. 긴 설명 제거
                if len(prompts) > THRESHOLD and clear_long_eg_des:
                    print(f"        🗜️  여전히 크기 초과, 긴 설명 제거 중...")
                    prompts = clear_description(prompts)
                    print(f"        ✂️  설명 제거 완료: {len(prompts):,} bytes")

                # 4-3. 외부 지식 추가
                if external_knowledge:
                    print(f"        📚 외부 지식 추가")
                prompts += f"도움이 될 수 있는 외부 지식: \n{external_knowledge}\n"

                # 4-4. 테이블 구조 정보 추가
                if not entry.startswith("local"):
                    prompts += "테이블 구조 정보 ({데이터베이스명: {스키마명: [테이블명]}}): \n" + str(table_dict) + "\n"
                    print(f"        📊 클라우드 DB 정보 추가: {project_count}개 프로젝트, {table_count}개 테이블")
                else:
                    prompts += "테이블 구조 정보 (테이블명들): \n" + str(table_names) + "\n"
                    print(f"        💾 SQLite DB 정보 추가: {len(table_names)}개 테이블")
                
                # 4-5. 최종 프롬프트 저장
                f.writelines(prompts)
                final_size = len(prompts)
                print(f"      ✅ 프롬프트 저장 완료: {final_size:,} bytes")

# 쓸 일 없을 것으로 보임
def get_sqlite_data(path, entry, add_description=False, add_sample_rows=False, gold_table_names=None, gold_column_names=None):
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    table_names = [table[0] for table in tables]
    prompts = ""
    for table in tables:
        table_name = table[0]

        if gold_table_names:
            if table_name.upper() not in gold_table_names:
                continue

        table_json = {}
        table_json["table_fullname"] = table_name

        cursor.execute("PRAGMA table_info({})".format(table_name))
        columns_info = cursor.fetchall()
        column_names = []
        column_types = []
        for col in columns_info:
            column_names.append(col[1])
            column_types.append(col[2])

        if gold_column_names:
            table_json["column_names"] = []
            table_json["column_types"] = []
            for i in range(len(column_names)):
                if column_names[i].upper() in gold_column_names:
                    table_json["column_names"].append(column_names[i])
                    table_json["column_types"].append(column_types[i])
        else:
            table_json["column_names"] = column_names
            table_json["column_types"] = column_types

        if not table_json["column_names"]:
            print(f"          ⚠️  컬럼 불일치: {table_name}")

        sample_rows = []
        if add_sample_rows:
            if gold_column_names:
                column_str = ", ".join(table_json["column_names"])
                query = f"SELECT {column_str} FROM {table_name} LIMIT 3"
            else:
                query = f"SELECT * FROM {table_name} LIMIT 3"
            # print(query)
            cursor.execute(query)
            sample_rows = cursor.fetchall()
        table_json["sample_rows"] = str(sample_rows)

        prompts += "\n" + "-" * 50 + "\n"
        prompts += "테이블 전체명: " + table_json["table_fullname"] + "\n"
        for j in range(len(table_json["column_names"])):
            table_des = ''
            prompts += "컬럼명: " + table_json["column_names"][j] + " 타입: " + table_json["column_types"][j] + table_des + "\n"
        if add_sample_rows:
            prompts += "샘플 데이터:\n" + table_json["sample_rows"] + "\n"
    connection.close()
    return table_names, prompts


if __name__ == '__main__':
    """
    type: 사용자가 입력한 값을 어떤 데이터 타입으로 변환할지
    action = "store_true": 인자가 있으면 `True`, 없으면 `False`
    """
    parser = argparse.ArgumentParser()
    
    # compress_ddl 함수의 인자
    parser.add_argument('--example_folder', type=str, default="examples")
    parser.add_argument('--add_description', action="store_true")
    parser.add_argument('--add_sample_rows', action="store_true")
    parser.add_argument('--rm_digits', action="store_true")
    parser.add_argument('--schema_linked', action="store_true")
    parser.add_argument('--clear_long_eg_des', action="store_true")
    parser.add_argument('--sqlite_sl_path', type=str, default=None)
    parser.add_argument('--reduce_col', action="store_true")
    parser.add_argument('--use_gold_table', action="store_true")
    parser.add_argument('--gold_table_pth', type=str, default=None) # 골드 테이블 파일 경로
    parser.add_argument('--use_gold_schema', action="store_true")
    parser.add_argument('--gold_sql_pth', type=str, default=None) # 골드 스키마 파일 경로

    # make_folder 함수의 인자
    parser.add_argument('--make_folder', action="store_true")
    
    args = parser.parse_args()
    if args.make_folder: # 현재: 사용 중 O
        make_folder(args)

    # 골드 테이블 로드: 정답 테이블 정보를 담은 JSON 파일 로드
    if args.use_gold_table: # 현재: 사용 중 X
        gold_tb = args.gold_table_pth
        with open(gold_tb) as f:
            gold = [json.loads(i) for i in f]

    # 골드 스키마 로드: 정답 SQL 파일들이 있는 경로 로드
    elif args.use_gold_schema: # 현재: 사용 중 X
        gold_sql_pth = args.gold_sql_pth

    # compress_ddl 함수 호출 (Main)
    print("=" * 60)
    print("🎉 DDL 압축 및 프롬프트 생성 완료!")
    print("=" * 60)
    
    compress_ddl(args.example_folder, 
                 args.add_description, args.add_sample_rows, 
                 args.rm_digits, args.schema_linked, args.clear_long_eg_des, 
                 args.sqlite_sl_path, args.reduce_col, 
                 args.use_gold_table, args.use_gold_schema)
    
    print("✅ 모든 작업이 완료되었습니다!")
    print(f"📁 생성된 프롬프트 파일들: {args.example_folder}/*/prompts.txt")
    print("🔧 다음 단계: schema_linking.py 실행")