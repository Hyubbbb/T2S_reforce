import os
import argparse
import glob
from utils import get_table_info, initialize_logger, get_dictionary, get_sqlite_path
from agent import REFORCE
from chat import GPTChat
from prompt import Prompts
import threading, concurrent
from sql import SqlEnv
import time
import json
import re

def execute(question, table_info, args, csv_save_path, log_save_path, sql_save_path, search_directory, format_csv, sql_data):
    """
    """
    print(f"      🔄 SQL 생성 시작: {sql_data}/{sql_save_path}")
    
    db_id = None
    if full_db_id:
        db_id = full_db_id[sql_data]

    # 1. SQL 환경 초기화
    sql_env = SqlEnv()
    # revote: execute sql if no csv
    if os.path.exists(os.path.join(search_directory, sql_save_path)) and not os.path.exists(os.path.join(search_directory, csv_save_path)) and args.revote:
        with open(os.path.join(search_directory, sql_save_path)) as f:
            sql = f.read()
        sql_env.execute_sql_api(sql, sql_data, os.path.join(search_directory, csv_save_path), sqlite_path=get_sqlite_path(args.db_path, sql_data, db_id, args.task))

    if args.rerun:
        if os.path.exists(os.path.join(search_directory, sql_save_path)):
            print(f"        ⏭️  Rerun 모드: 기존 결과 존재, 스킵")
            return
        else:
            print(f"        🔄 Rerun 모드: 새로 생성 필요")
    # if log.log exists, pass
    elif os.path.exists(os.path.join(search_directory, log_save_path)):
        print(f"        ⏭️  로그 파일 존재, 스킵")
        return

    # remove files
    self_files = glob.glob(os.path.join(search_directory, f'*{log_save_path}*'))
    for self_file in self_files:
        os.remove(self_file)

    # log
    log_file_path = os.path.join(search_directory, log_save_path)
    logger = initialize_logger(log_file_path)
    if format_csv:
        logger.info("[답변 형식]\n" + format_csv + "\n[답변 형식]")
    table_struct = table_info[table_info.find("테이블 구조 정보"):]

    # 2. GPT 채팅 세션 초기화
    print(f"        🤖 GPT 세션 초기화 중...")
    chat_session_ex = None # Column Exploration용 채팅 세션
    chat_session = None    # 최종 SQL Generation용 채팅 세션
    if args.do_column_exploration:
        chat_session_ex = GPTChat(args.azure, args.column_exploration_model, temperature=args.temperature)
        print(f"        🔍 Column Exploration 세션 준비 완료: {args.column_exploration_model}")
    if args.generation_model:
        chat_session = GPTChat(args.azure, args.generation_model, temperature=args.temperature)
        print(f"        ⚡ SQL Generation 세션 준비 완료: {args.generation_model}")

    # 3. REFORCE 에이전트 생성
    agent = REFORCE(
        args.db_path,      # "examples_fnf" 
        sql_data,          # "fnf001"
        search_directory,  # "output/o3-fnf-no-exploration-log-20241215-143022/fnf001"
        prompt_all,        # Prompts() 인스턴스
        sql_env,           # SQL 실행 환경
        chat_session_ex,   # Column Exploration용 GPT 세션
        chat_session,      # SQL Generation용 GPT 세션
        sql_data+'/'+log_save_path,  # 로그 식별자
        db_id,             # 데이터베이스 ID
        task=args.task     # 태스크 정보
    )

    # do_column_exploration
    pre_info, response_pre_txt = None, None
    # 4. 컬럼 탐색 수행 (옵션)
    if args.do_column_exploration:
        print(f"        🔍 Column Exploration 시작...")
        pre_info, response_pre_txt, max_try = agent.exploration(question, table_struct, table_info, logger)
        if max_try <= 0:
            print(f"        ❌ Column Exploration 실패: {sql_data+'/'+log_save_path} 준비 부족, 건너뜀")
            return
        print(f"        ✅ Column Exploration 완료: 채팅 세션 길이 {chat_session_ex.get_message_len()}")

    csv_save_path = os.path.join(search_directory, csv_save_path)
    sql_save_path = os.path.join(search_directory, sql_save_path)

    # 5. Self-refinement 또는 단순 생성
    if args.do_self_refinement:
        print(f"        🔄 Self-refinement 시작 (최대 {args.max_iter}회 반복)...")
        agent.self_refine(args, logger, question, format_csv, table_struct, table_info, response_pre_txt, pre_info, csv_save_path, sql_save_path, task=args.task)
        print(f"        ✅ Self-refinement 완료")
    elif args.generation_model:
        print(f"        🎯 단순 SQL Generation 시작...")
        agent.gen(args, logger, question, format_csv, table_struct, table_info, response_pre_txt, pre_info, csv_save_path, sql_save_path, task=args.task)
        print(f"        ✅ SQL Generation 완료")
    
    if args.generation_model:
        agent.sql_env.close_db()
    
    print(f"      ✅ SQL Generation 완료: {sql_data}/{sql_save_path}")

def main(args):
    print("=" * 80)
    print("🚀 ReFoRCE 메인 프로세스 시작")
    print("=" * 80)
    print(f"📊 처리할 인스턴스 수: {len(dictionaries)}개")
    print(f"🔧 병렬 num_workers: {args.num_workers}개")
    print(f"🎯 Task: {args.task}")
    print(f"📂 출력 경로: {args.output_path}")
    print()
    
    # Use ThreadPoolExecutor to process each sql_data in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.num_workers) as executor:
        list(executor.map(process_sql_data, dictionaries))

    print()
    print("=" * 80)
    print("🎉 모든 인스턴스 처리 완료!")
    print("=" * 80)

def process_sql_data(sql_data): # sql_data = "sf_bq070"
    """
    개별 인스턴스 처리 함수

    Args:
        sql_data (str): 처리할 인스턴스 ID

    Returns:
        None
    """
    # 초기 설정
    start_time = time.time()

    print(f"📋 [{sql_data}] 인스턴스 처리 시작")

    question = task_dict[sql_data] # "Find top 5 customers..."
    search_directory = os.path.join(args.output_path, sql_data) # "output/o3-snow-log/sf_bq070"

    # Create agent object
    agent_format = REFORCE(args.db_path, sql_data, search_directory, prompt_all)
    
    # Create the directory if it does not exist
    if not os.path.exists(search_directory):
        os.makedirs(search_directory)
        print(f"    📁 출력 디렉토리 생성: {search_directory}")

    # Skip processing if results already exist and overwrite is not allowed
    if os.path.exists(agent_format.complete_sql_save_path) and not args.revote:
        print(f"    ⏭️  최종 결과 존재, 스킵: {agent_format.complete_sql_save_path}")
        return
    
    if args.overwrite_unfinished:
        if not os.path.exists(agent_format.complete_sql_save_path):
            print(f"    🗑️  미완료 파일들 정리 중...")
            for filename in os.listdir(search_directory):
                filepath = os.path.join(search_directory, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            print(f"    ✅ 미완료 파일들 정리 완료")
        else:
            print(f"    ⏭️  완료된 결과 존재, 스킵")
            return

    # Ensure the search directory exists (in case it was removed)
    if not os.path.exists(search_directory):
        os.makedirs(search_directory)

    # # sqlite task
    # if args.subtask == "sqlite":
    #     if not sql_data.startswith("local"):
    #         print(f"    ⏭️  SQLite 서브태스크: local이 아닌 인스턴스 스킵")
    #         return

    # # Get BIRD gold res
    # if args.task == "BIRD":
    #     gold_pth = os.path.join(args.BIRD_gold_result_path, sql_data+".csv")
    #     if not os.path.exists(gold_pth):
    #         print(f"    🏆 BIRD 골드 결과 생성 중...")
    #         sql_env = SqlEnv()
    #         res = sql_env.execute_sql_api(full_gold_sql[sql_data], sql_data, gold_pth, sqlite_path=get_sqlite_path(args.db_path, sql_data, full_db_id[sql_data], args.task), timeout=1200)
    #         assert res == "0", (sql_data, res)
    #         print(f"    ✅ BIRD 골드 결과 생성 완료")

    # Get table information
    print(f"    📊 테이블 정보 로드 중...")
    table_info = get_table_info(args.db_path, sql_data, agent_format.api, clear_des=True, full_tb_info=full_tb_info)
    if len(table_info) > 300000:
        print(f"    ⚠️  테이블 정보 크기 초과: {len(table_info):,} bytes, 반환")
        return
    print(f"    ✅ 테이블 정보 로드 완료: {len(table_info):,} bytes")

    # Format restriction 처리
    format_csv = None
    if args.do_format_restriction:
        print(f"    📝 답변 형식 제한 처리 중...")
        if args.use_gold_format:
            csv_pth = os.path.join("../../spider2-lite/evaluation_suite/gold/exec_result", sql_data+".csv")
            if not os.path.exists(csv_pth):
                csv_pth = csv_pth.replace(".csv", "_a.csv")
            with open(csv_pth) as f:
                format_csv = "```sql\n"+f.read().split("\n")[0]+"\n```"
            print(f"    ✅ 골드 형식 로드 완료")
        else:
            # Initialize sessions at the beginning of each thread
            chat_session_format = GPTChat(args.azure, args.format_model, temperature=args.temperature)
            # Format answer and update the pre-chat session
            format_csv = agent_format.format_answer(question, chat_session_format)
            print(f"    ✅ 형식 생성 완료")

    # 투표 모드 vs 단일 모드 처리
    if args.do_vote:
        print(f"    🗳️  투표 모드 시작 ({args.num_votes}번 시도)")
        num_votes = args.num_votes
        sql_paths = {}
        threads = []

        for i in range(num_votes):
            csv_save_pathi = str(i) + agent_format.csv_save_name    # "0result.csv", "1result.csv", "2result.csv" (각각 첫 번째, 두 번째, 세 번째 시도)
            log_pathi = str(i) + agent_format.log_save_name         # "0log.log", "1log.log", "2log.log"
            sql_save_pathi = str(i) + agent_format.sql_save_name    # "0result.sql", "1result.sql", "2result.sql"
            sql_paths[sql_save_pathi] = csv_save_pathi 

            # 각각을 별도 스레드에서 실행
            thread = threading.Thread(
                target=execute,
                args=(
                    question, table_info, args,
                    csv_save_pathi, log_pathi, sql_save_pathi,
                    search_directory, format_csv, sql_data
                )
            )
            threads.append(thread)
            thread.start()

        print(f"    ⏳ {num_votes}개 스레드 실행 대기 중...")
        # wait
        for thread in threads:
            thread.join()
        print(f"    ✅ 모든 스레드 실행 완료")
        
        # 재투표 처리
        if args.revote:
            print(f"    🔄 재투표 모드: 기존 결과 제거")
            if "result.sql" in os.listdir(search_directory):
                print(f"        🗑️  기존 result.sql 제거: {os.path.join(search_directory, 'result.sql')}")
                os.remove(os.path.join(search_directory, "result.sql"))
            if "result.csv" in os.listdir(search_directory):
                print(f"        🗑️  기존 result.csv 제거: {os.path.join(search_directory, 'result.csv')}")
                os.remove(os.path.join(search_directory, "result.csv"))
        
        # 투표 결과 처리
        if "result.sql" not in os.listdir(search_directory):
            sql_files = [f for f in os.listdir(search_directory) if f.endswith('.sql') and os.path.isfile(os.path.join(search_directory, f))]
            if sql_files:
                print(f"    🗳️  투표 시작: {len(sql_files)}개 SQL 파일 발견")
                print(f"        📁 SQL 파일들: {sql_files}")
                # After all processes have completed, perform the vote result
                agent_format.vote_result(search_directory, args, sql_paths, table_info, question)
                
                # 투표 결과 확인
                if os.path.exists(os.path.join(search_directory, "result.sql")):
                    print(f"    ✅ 투표 완료: result.sql 생성됨")
                else:
                    print(f"    ❌ 투표 실패: result.sql 생성되지 않음")
            else:
                print(f"    ❌ 투표 불가: SQL 파일이 없음")
    else:
        print(f"    🎯 단일 모드 실행")
        # Directly execute the task
        execute(
            question, table_info, args,
            agent_format.csv_save_name, agent_format.log_save_name, agent_format.sql_save_name,
            search_directory, format_csv, sql_data
        )

    elapsed_time = int((time.time() - start_time) // 60)
    print(f"✅ [{sql_data}] 처리 완료 (소요시간: {elapsed_time}분)")
    print()

if __name__ == '__main__':
    # 1-1. 인자 파싱
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', type=str, default="snow", choices=["snow", "lite", "BIRD", "fnf"],)
    parser.add_argument('--subtask', type=str, default=None, choices=["sqlite"])
    parser.add_argument('--db_path', type=str, default=None)
    parser.add_argument('--output_path', type=str, default="output/o3-snow-log")

    parser.add_argument('--do_format_restriction', action="store_true")
    parser.add_argument('--use_gold_format', action="store_true")
    parser.add_argument('--format_model', type=str, default="o3")

    parser.add_argument('--do_column_exploration', action="store_true")
    parser.add_argument('--column_exploration_model', type=str, default="o3")    

    parser.add_argument('--do_self_refinement', action="store_true")
    parser.add_argument('--do_self_consistency', action="store_true")
    parser.add_argument('--generation_model', type=str, default=None)
    parser.add_argument('--azure', action="store_true")

    parser.add_argument('--max_iter', type=int, default=5)
    parser.add_argument('--temperature', type=float, default=1)
    parser.add_argument('--early_stop', action="store_true")

    parser.add_argument('--do_vote', action="store_true")
    parser.add_argument('--revote', action="store_true")
    parser.add_argument('--num_votes', type=int, default=3)
    parser.add_argument('--random_vote_for_tie', action="store_true")
    parser.add_argument('--model_vote', type=str, default=None)
    parser.add_argument('--final_choose', action="store_true")

    parser.add_argument('--save_all_results', action="store_true")
    parser.add_argument('--rerun', action="store_true")
    parser.add_argument('--overwrite_unfinished', action="store_true")
    parser.add_argument('--num_workers', type=int, default=16)

    parser.add_argument('--omnisql_format_pth', type=str, default=None)
    parser.add_argument('--BIRD_gold_result_path', type=str, default="../../data/BIRD/gold_result")

    args = parser.parse_args()

    # 1-2. 전역 변수 초기화
    prompt_all = Prompts()  # 프롬프트 템플릿 코드
    full_db_id = {}         # 데이터베이스 ID 매핑
    full_tb_info = {}       # 테이블 정보 캐시

    full_gold_sql = {}      # 골드 SQL (BIRD 태스크용)

    # # 2-1. 특수 형식 파일 처리 (omnisql_format_pth가 있는 경우)
    # if args.omnisql_format_pth:
    #     # SQLite 또는 BIRD 태스크용 특별 처리
        
    #     # SQLite 태스크용 처리
    #     if args.subtask == "sqlite":
    #         with open(args.omnisql_format_pth) as f:
    #             data = json.load(f)
    #         dictionaries = []
    #         task_dict = {}
    #         full_tb_info = {}
            
    #         for example in data:
    #             if example["instance_id"].startswith("local"):
    #                 dictionaries.append(example["instance_id"])
    #                 task_dict[example["instance_id"]] = example["question"]
    #                 full_tb_info[example["instance_id"]] = example["db_desc"]
    #                 full_db_id[example["instance_id"]] = example["db_id"]

    #     # BIRD 태스크용 처리
    #     elif args.task == "BIRD":
    #         with open(args.omnisql_format_pth) as f:
    #             data = json.load(f)
    #         dictionaries = []
    #         task_dict = {}
    #         full_tb_info = {}
            
    #         for example in data:
    #             q_id = example["question_id"]
    #             instance_id = f"local_BIRD_{q_id:04d}"
    #             dictionaries.append(instance_id)
    #             task_dict[instance_id] = example["question"]
    #             full_tb_info[instance_id] = example["input_seq"]
    #             full_db_id[instance_id] = example["db_id"]     
    #             full_gold_sql[instance_id] = example["SQL"]                     
    # else:
    #     # 일반적인 경우: 기본 딕셔너리 및 태스크 정보 로드
    #     dictionaries, task_dict = get_dictionary(args.db_path, args.task)
    # 일반적인 경우: 기본 딕셔너리 및 태스크 정보 로드
    dictionaries, task_dict = get_dictionary(args.db_path, args.task)

    # 2-2. 메인 프로세스 실행 (병렬 처리)
    main(args)