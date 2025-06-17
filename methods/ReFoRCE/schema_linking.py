from utils import search_file, get_api_name, get_dictionary, get_tb_info, get_external, compute_precision_recall, is_csv_empty, clear_name
from reconstruct_data import remove_digits, compress_ddl
import os
import json
import csv
from tqdm import tqdm
from chat import GPTChat
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import sys
import re
import numpy as np

# Windows 환경에서 오류 발생 시 32비트 시스템 최대값으로 설정
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2147483647)  # 32비트 시스템 최대값
    
# THRESHOLD = 200000
THRESHOLD = 50000  # 50KB로 낮춤 (FNF 태스크 고려)
DEPS_DEV_V1 = ["sf_bq016", "sf_bq062", "sf_bq063", "sf_bq028"]

def reduce_columns(sql: str, subset_columns: set[str]) -> str:

    table_match = re.search(r'create\s+(?:or\s+replace\s+)?table\s+`?([^\s(]+)`?', sql, re.IGNORECASE)
    assert table_match, sql
    table_name = table_match.group(1)

    # 괄호 매칭으로 컬럼 블록 추출 (정규식보다 안정적)
    start_idx = sql.find('(')
    if start_idx == -1:
        raise ValueError("Cannot find opening parenthesis.")
    
    # 괄호 매칭으로 끝 찾기
    paren_count = 0
    end_idx = start_idx
    
    for i, char in enumerate(sql[start_idx:], start_idx):
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
            if paren_count == 0:
                end_idx = i
                break
    
    if paren_count != 0:
        raise ValueError("Cannot find matching closing parenthesis.")
    
    columns_block = sql[start_idx+1:end_idx]

    lines = columns_block.splitlines()
    filtered_lines = []
    for line in lines:
        line = line.strip().rstrip(',')
        if not line:
            continue
        # primary key 같은 제약조건은 스킵
        if line.lower().startswith('primary key') or line.lower().startswith('foreign key') or line.lower().startswith('unique'):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        col_name = parts[0].strip('`",')
        if col_name in subset_columns:
            filtered_lines.append(f"  {line},")

    if filtered_lines:
        filtered_lines[-1] = filtered_lines[-1].rstrip(',')

    new_sql = f'CREATE TABLE {table_name} (\n' + '\n'.join(filtered_lines) + '\n);'
    return new_sql


def reduce_ddl(example_path, dictionaries, linked_json, reduce_col=False):
    # 스키마 링킹 결과를 바탕으로 불필요한 테이블 제거
    # 컬럼 레벨 필터링으로 추가 압축
    
    print(f"📋 DDL 축소 대상: {len(dictionaries)}개 예제")
    print(f"📄 스키마 링킹 결과 파일: {linked_json}")
    print(f"🎯 임계값: {THRESHOLD:,} bytes 이상인 프롬프트만 처리")
    print()
    
    processed_count = 0
    skipped_count = 0
    
    for eg_id in tqdm(dictionaries, desc="📂 DDL 축소 처리"):
        api = get_api_name(eg_id)
 
        ddl_paths = search_file(os.path.join(example_path, eg_id), "DDL.csv")

        # 1. 프롬프트 크기 확인 (프롬프트 파일이 200KB 이상이거나, 개발 데이터셋인 경우 건너뜀)
        prompt_size = os.path.getsize(os.path.join(example_path, eg_id, "prompts.txt"))
        if prompt_size < THRESHOLD or eg_id in DEPS_DEV_V1:
            skipped_count += 1
            continue

        # 2. 스키마 링킹 결과 로드
        with open(linked_json, encoding='utf-8') as f:
            sl = json.load(f)

        # 3. 스키마 링킹 결과를 통해, 파싱하여 테이블 이름 추출
        table_names = []
        columns = {}
        for ex_id, tbs in sl.items():
            if ex_id == eg_id:
                for tb in tbs:
                    if "answer" in tb:
                        if tb["answer"] == "Y": # 관련 있다고 판단된 테이블만
                            table_names.append(tb["table name"])
                            columns[tb["table name"]] = tb['columns']
                    else:
                        raise NotImplementedError
                        print(tb)
                        table_names.append(tb)

        if not table_names:
            print(f"      ⚠️  {eg_id}: 관련 테이블 없음, 스킵")
            skipped_count += 1
            continue
        
        processed_count += 1
        table_names_no_digit = [remove_digits(i) for i in table_names]

        temp_file_paths = []
        # 4. DDL.csv → DDL_sl.csv 필터링
        for ddl_path in ddl_paths:
            temp_file = ddl_path.replace("DDL.csv", "DDL_sl.csv")
            temp_file_paths.append(temp_file)
            with open(ddl_path, "r", newline="", encoding="utf-8", errors="ignore") as infile, \
                open(temp_file, "w", newline="", encoding="utf-8", errors="ignore") as outfile:
                
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                header = next(reader)
                writer.writerow(header)
                row_count = 0
                row_count_rm = 0
                total_count = 0
                row_list_all = []
                row_list = []
                for row in reader:
                    assert row[-1].upper().startswith("CREATE"), row
                    if "." in row[0]:
                        row[0] = row[0].split(".")[-1]

                    json_pth = ddl_path.replace("DDL.csv", row[0].strip()+".json")
                    if os.path.exists(json_pth):
                        with open(json_pth, encoding="utf-8") as f:
                            table_fullname = json.load(f)["table_fullname"]
                    else:
                        print(f"{ex_id}: {json_pth} doesn't exist")
                        continue
                    
                    if any(remove_digits(table_fullname) in item for item in table_names_no_digit):
                        row_count_rm += 1
                        row_list_all.append(row)

                    if any(table_fullname == item for item in table_names):

                        row_count += 1

                        if reduce_col:
                            assert table_fullname in columns, print(table_names, table_fullname)
                            row[-1] = reduce_columns(row[-1], columns[table_fullname])
                            # print("After", row)
                        row_list.append(row)
                    total_count += 1
                # 결과 통계 출력 및 파일 작성 결정
                if 0 < row_count < 10 or row_count_rm > 1000 or reduce_col:
                    writer.writerows(row_list)
                    result_type = "정확한 매칭"
                elif row_count_rm:
                    writer.writerows(row_list_all)
                    result_type = "숫자 제거 매칭"
                else:
                    result_type = "매칭 없음"
                
        # 빈 DDL 파일 정리
        if all(is_csv_empty(i) for i in temp_file_paths):
            for i in temp_file_paths:
                os.remove(i)
    print()
    print(f"📊 DDL 축소 완료 통계:")
    print(f"   - 처리된 예제: {processed_count}개")
    print(f"   - 스킵된 예제: {skipped_count}개")
    print(f"   - 총 예제: {len(dictionaries)}개")
    print()
    
    print("🔄 축소된 DDL을 바탕으로 프롬프트 재생성 중...")
    compress_ddl(example_path, add_description=True, add_sample_rows=False, rm_digits=True, schema_linked=True, clear_long_eg_des=True, reduce_col=reduce_col)
    # compress_ddl(example_path, add_description=True, add_sample_rows=True, rm_digits=True, schema_linked=True, clear_long_eg_des=True, reduce_col=reduce_col)
    print("✅ 프롬프트 재생성 완료")

ask_prompt = """
테이블 수준 스키마 링킹을 수행하고 있습니다. 스키마 정보가 있는 테이블과 작업이 주어졌을 때, 단계별로 생각하여 이 테이블이 작업과 관련이 있는지 결정해야 합니다.
Y/N으로만 답해야 합니다. 답이 Y인 경우, 관련 있다고 생각하는 컬럼들을 파이썬 리스트 형식으로 추가해야 합니다.

다음과 같은 json 코드 블록으로만 답해주세요:
```json
{{
    "think": "결정하기 위해 단계별로 생각",
    "answer": "Y 또는 N만",
    "columns": [col_name1, col_name2]
}}
```

테이블 정보: {0}
작업: {1}
{2}
"""

def ask_model_sl(example_path, json_save_pth):
    """
    Schema linking을 수행하는 함수
    Args:
        example_path (str): 예제 폴더 경로
        json_save_pth (str): 스키마 링킹 결과를 저장할 파일 경로

    Returns:
        linked_dic (dict): 스키마 링킹 결과
    """
    linked_dic = {}

    def process_example(ex_id):
        if ex_id.startswith("local"): # SQLite 기반 local 예제는 스키마 링킹에서 제외 (Local 에제는 이미 작은 규모)
            return None, None
        
        tb_info_pth = search_file(os.path.join(example_path, ex_id), "prompts.txt") # 프롬프트 파일 경로 찾기
        assert len(tb_info_pth) == 1
        with open(tb_info_pth[0], encoding="utf-8") as f:
            tb_info = f.read()

        task = task_dict[ex_id]
        chat_session = GPTChat(azure=False, model="gpt-4o", temperature=0)
        result = ask_model_sl_(tb_info, task, chat_session)
        return ex_id, result

    linked_dic = {}
    print("Doing table-level schema linking")

    # 32개의 스레드를 사용하여 병렬 처리
    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process_example, ex_id) for ex_id in dictionaries]

        processed_count = 0
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔗 테이블 관련성 분석"):
            ex_id, result = future.result() # process_example의 결과 반환
            if ex_id is not None: # 예외 처리
                linked_dic[ex_id] = result
                processed_count += 1

        print(f"✅ 스키마 링킹 완료: {processed_count}개 예제 처리")
        print(f"💾 결과 저장 중: {json_save_pth}")
        
        with open(json_save_pth, "w", encoding="utf-8") as f:
            json.dump(linked_dic, f, indent=4, ensure_ascii=False)

def ask_model_sl_(tb_info, task, chat_session): # sl: schema linking
    """
    GPT 모델을 사용하여 각 테이블이 질문과 관련있는지 판단하고,
    Y/N 답변과 함께 관련 컬럼 목록 반환하는 함수
    Args:
        tb_info (str): 테이블 정보
        task (str): 작업
        chat_session (GPTChat): GPT 모델 세션

    Returns:
        linked (list): 스키마 링킹 결과
    """
    tbs = get_tb_info(tb_info)
    external = get_external(tb_info)
    linked = []

    for tb in tbs:
        chat_session.init_messages()
        max_try = 3
        input = ask_prompt.format(tb, task, external)
        while max_try:
            response = chat_session.get_model_response(input, "json")
            if len(response) == 1:
                response = response[0]
                try:
                    data = json.loads(response)
                    assert data["answer"] in ["Y", "N"], 'data["answer"] should be in ["Y", "N"]'
                    # 영어와 한글 테이블명 모두 지원
                    table_name_match = re.search(r'^Table full name:\s*(.+)$', tb, re.MULTILINE)
                    if not table_name_match:
                        table_name_match = re.search(r'^테이블 전체명:\s*(.+)$', tb, re.MULTILINE)
                    data["table name"] = table_name_match.group(1)
                    break
                except Exception as e:
                    input = e+"Please generate again."
            max_try -= 1
        if max_try == 0:
            # 영어와 한글 테이블명 모두 지원
            table_name_match = re.search(r'^Table full name:\s*(.+)$', tb, re.MULTILINE)
            if not table_name_match:
                table_name_match = re.search(r'^테이블 전체명:\s*(.+)$', tb, re.MULTILINE)
            if table_name_match:
                print("Failed", table_name_match.group(1))
            else:
                print("Failed to extract table name from:", tb[:100])
            continue
        # print(data)
        linked.append(data)

    return linked

def compute_metrics_sl(file_pth, db_path):
    print(f"📊 성능 평가 파일 로드: {file_pth}")
    with open(file_pth, encoding="utf-8") as f:
        data = json.load(f)
    
    count = 0
    precision_all = []
    recall_all = []
    perfect_recall_count = 0
    
    print(f"📋 평가 대상: {len(data)}개 예제")
    
    for example, tbs in data.items():
        # 골드 테이블 찾기
        gold_table = None
        for ex in gold:
            if ex['instance_id'] == example:
                gold_table = set(ex["gold_tables"])
                break
        
        if gold_table is None:
            continue

        # 임계값 이상인 예제만 평가
        if os.path.getsize(os.path.join(db_path, example, "prompts.txt")) > THRESHOLD:
            count += 1
            pred = []
            
            # 예측된 테이블 추출
            for tb in tbs:
                if "answer" in tb:
                    if tb["answer"] == "Y":
                        pred.append(tb["table name"])
                else:
                    print(f"      ⚠️  예상치 못한 형식: {tb}")
                    pred.append(tb)
            
            # Precision/Recall 계산
            precision, recall = compute_precision_recall(clear_name(pred), clear_name(gold_table))
            
            if precision != 0 and recall != 0:
                if recall == 1.0:
                    perfect_recall_count += 1
                else:
                    print(f"      📉 불완전한 Recall: {example} (P: {precision:.3f}, R: {recall:.3f})")
                
                precision_all.append(precision)
                recall_all.append(recall)
    
    # 최종 통계
    if precision_all and recall_all:
        mean_precision = np.mean(precision_all)
        mean_recall = np.mean(recall_all)
        imperfect_recall_count = np.sum(np.array(recall_all) < 1)
        
        print()
        print("📊 스키마 링킹 성능 평가 결과:")
        print(f"   - 평가된 예제 수: {count}개")
        print(f"   - 평균 Precision: {mean_precision:.3f}")
        print(f"   - 평균 Recall: {mean_recall:.3f}")
        print(f"   - 완벽한 Recall (1.0): {perfect_recall_count}개")
        print(f"   - 불완전한 Recall (<1.0): {imperfect_recall_count}개")
    else:
        print("⚠️  평가할 수 있는 데이터가 없습니다.")  

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', type=str, default="lite")
    parser.add_argument('--db_path', type=str, default="examples_lite_full")
    parser.add_argument('--linked_json_pth', type=str, default=None)
    parser.add_argument('--reduce_col', action="store_true")
    parser.add_argument('--gold_tb_pth', type=str, default=None)
    args = parser.parse_args()

    print("🎯 Schema Linking 및 DDL 축소를 시작합니다")
    print(f"📂 대상 폴더: {args.db_path}")
    print(f"🏷️  태스크: {args.task}")
    print(f"📄 스키마 링킹 결과 파일: {args.linked_json_pth}")
    print(f"🔧 컬럼 축소: {'✅' if args.reduce_col else '❌'}")
    print(f"🏆 골드 테이블 파일: {args.gold_tb_pth if args.gold_tb_pth else '❌ 없음'}")
    print()

    print("=" * 60)
    print("📋 STEP 1: 딕셔너리 및 태스크 정보 로드")
    print("=" * 60)
    dictionaries, task_dict = get_dictionary(args.db_path, args.task)
    print(f"✅ 딕셔너리 로드 완료: {len(dictionaries)}개 예제")
    print(f"✅ 태스크 정보 로드 완료: {len(task_dict)}개 질문")
    print()
    
    # 스키마 링킹 수행 (linked_json_pth가 존재하지 않을 때만) -- 비싼 작업이라, 이미 수행된 경우 건너뜀
    if args.linked_json_pth is not None and not os.path.exists(args.linked_json_pth):
        print("=" * 60)
        print("🧠 STEP 2: Schema Linking 수행")
        print("=" * 60)
        print(f"📄 결과 파일이 존재하지 않음: {args.linked_json_pth}")
        print("🚀 GPT 모델을 사용한 스키마 링킹을 시작합니다...")
        print()
        
        gold = []  # 기본값으로 빈 리스트 설정
        
        # 골드 테이블 파일이 제공되고 존재하는 경우에만 로드
        print("🏆 골드 테이블 로드 시도 중...")
        if args.gold_tb_pth is not None and os.path.exists(args.gold_tb_pth):
            try:
                with open(args.gold_tb_pth, encoding="utf-8") as f:
                    gold = [json.loads(i) for i in f]
                print(f"✅ 골드 테이블 로드 완료: {len(gold)}개 인스턴스")
            except Exception as e:
                print(f"❌ 골드 테이블 로드 실패: {e}")
                print("⚠️  골드 테이블 없이 진행합니다.")
                gold = []
        else:
            if args.gold_tb_pth is None:
                print("ℹ️  골드 테이블 경로가 제공되지 않았습니다. 골드 테이블 없이 진행합니다.")
            else:
                print(f"❌ 골드 테이블 파일이 존재하지 않습니다: {args.gold_tb_pth}")
                print("⚠️  골드 테이블 없이 진행합니다.")
        print()

        # 스키마 링킹 수행
        print("🔗 테이블-태스크 관련성 분석 시작...")
        ask_model_sl(args.db_path, args.linked_json_pth)
        print(f"✅ 스키마 링킹 결과 저장 완료: {args.linked_json_pth}")
        print()

        # 골드 데이터가 있을 때만 메트릭 계산
        if gold:
            print("📊 성능 평가 시작...")
            print("🏆 골드 테이블을 사용하여 스키마 링킹 성능을 평가합니다.")
            compute_metrics_sl(args.linked_json_pth, args.db_path)
            print("✅ 성능 평가 완료")
        else:
            print("ℹ️  골드 테이블이 없어 성능 평가를 생략합니다.")
        print()
    else:
        print("=" * 60)
        print("⏭️  STEP 2: Schema Linking 스킵")
        print("=" * 60)
        if args.linked_json_pth is None:
            print("ℹ️  스키마 링킹 결과 파일 경로가 제공되지 않았습니다.")
        else:
            print(f"✅ 기존 스키마 링킹 결과 파일 발견: {args.linked_json_pth}")
            print("💰 비용 절약을 위해 기존 결과를 재사용합니다.")
        print()
    
    # DDL 축소 수행 (항상 실행)
    print("=" * 60)
    print("✂️  STEP 3: DDL 축소 및 프롬프트 재생성")
    print("=" * 60)
    print("🗄️  스키마 링킹 결과를 바탕으로 불필요한 테이블을 제거합니다...")
    print(f"🔧 컬럼 레벨 축소: {'✅ 활성화' if args.reduce_col else '❌ 비활성화'}")
    print()
    
    reduce_ddl(args.db_path, dictionaries, args.linked_json_pth, args.reduce_col)
    
    print("🎉 모든 작업이 완료되었습니다!")
    print(f"📁 축소된 DDL 파일들: {args.db_path}/*/DDL_sl.csv")
    print(f"📄 재생성된 프롬프트들: {args.db_path}/*/prompts.txt")
    print("🔧 다음 단계: run.py 실행")