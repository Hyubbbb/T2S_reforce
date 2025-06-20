from utils import hard_cut, get_values_from_table, get_api_name, filter_bijection_like_dict, compare_pandas_table, is_valid_result, get_sqlite_path, split_sql_safe
from sql import SqlEnv
import pandas as pd
from io import StringIO
import os
import shutil
import csv
from prompt import Prompts
from typing import Type
from chat import GPTChat
import sys
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2147483647)  # 32비트 시스템 최대값

class REFORCE:
    def __init__(self, db_path, sql_data, search_directory, prompt_class: Type[Prompts], sql_env: Type[SqlEnv]=None, chat_session_pre: Type[GPTChat]=None, chat_session: Type[GPTChat]=None, log_save_path=None, db_id=None, task=None):
        # 초기화: API 설정, 경로 설정, 프롬프트 클래스 등
        self.csv_save_name = "result.csv"     # 최종 CSV 결과 파일명
        self.sql_save_name = "result.sql"     # 최종 SQL 결과 파일명  
        self.log_save_name = "log.log"        # 로그 파일명
        self.log_vote_name = "vote.log"       # 투표 로그 파일명
        self.empty_result = "No data found for the specified query.\n"

        # API 종류 결정 (sql_data 기반)
        self.api = get_api_name(sql_data)
        self.sqlite_path = get_sqlite_path(db_path, sql_data, db_id, task) # SQLite인 경우

        # 로그 ID 설정
        self.sql_id = log_save_path

        # 완전 경로 생성
        self.complete_csv_save_path = os.path.join(search_directory, self.csv_save_name) # CSV 결과 저장 경로
        # "output/o3-fnf-no-exploration-log-20241215-143022/fnf001/result.csv"
        self.complete_sql_save_path = os.path.join(search_directory, self.sql_save_name) # SQL 결과 저장 경로
        # "output/o3-fnf-no-exploration-log-20241215-143022/fnf001/result.sql"
        self.complete_log_save_path = os.path.join(search_directory, self.log_save_name) # 로그 저장 경로
        # "output/o3-fnf-no-exploration-log-20241215-143022/fnf001/log.log"
        self.complete_vote_log_path = os.path.join(search_directory, self.log_vote_name) # 투표 로그 저장 경로
        # "output/o3-fnf-no-exploration-log-20241215-143022/fnf001/vote.log"

        self.prompt_class = prompt_class        # Prompts() 인스턴스
        self.max_try = 3                        # 최대 재시도 횟수
        self.csv_max_len = 500                  # CSV 최대 길이

        self.sql_env = sql_env
        self.chat_session_pre = chat_session_pre
        self.chat_session = chat_session


    def execute_sqls(self, sqls, logger):
        """
        SQL 실행 및 결과 처리 (여러 sql을 순차적으로 실행하고 결과를 처리)
        - 성공한 경우: 결과를 저장하고 다음으로
        - 실패한 경우: `self_correct` 함수를 통해 오류 수정 시도

        Args:
            sqls: 실행할 SQL 목록
            logger: 로깅 객체

        Returns:
            result_dic_list: { 'sql':..., 'res':... } 형태의 실행 결과 딕셔너리 리스트
        """

        # 1. 초기화
        result_dic_list = []
        error_rec = []

        # 2. 모든 SQL 실행
        while sqls:
            # 2-1. 10개 이상 실행 시 종료
            if len(result_dic_list) > 10:
                break
            result_dic = {} # 실행 결과 저장
            sql = sqls[0]
            sqls = sqls[1:] # 실행할 SQL 문자열 리스트
            logger.info("[SQL 실행 시도]\n" + sql + "\n[SQL 실행 시도]")

            # 2-2. SQL 실행
            results = self.sql_env.execute_sql_api(sql, self.sql_id, api=self.api, max_len=self.csv_max_len, sqlite_path=self.sqlite_path)

            # 2-3. 성공 시 결과 저장
            if isinstance(results, str) and results != self.empty_result:
                result_dic['sql'] = sql
                result_dic['res'] = results
                # self.chat_session_pre.messages.append({"role": "user", "content": f"Successfully executed. SQL:\n{sql}\nResults:\n{results}"})
                logger.info("[성공적으로 실행됨]\n" +  f"성공적으로 실행됨. SQL:\n{sql}\n결과:\n{results}" + "\n[성공적으로 실행됨]")
                result_dic_list.append(result_dic)
            else:
                # 2-4. 실패 시 오류 수정
                logger.info("[오류 발생]\n" + str(results) + "\n[오류 발생]")
                max_try = self.max_try
                simplify = False
                corrected_sql = None

                # 2-5. 오류 수정 시도
                while not isinstance(results, str) or results == self.empty_result:
                    error_rec.append(0)
                    if max_try == 0:
                        break
                    if results == self.empty_result:
                        simplify = True

                    # 2-5-1. 오류 수정 시도
                    corrected_sql = self.self_correct(sql, results, logger, simplify=simplify)
                    
                    # 2-5-2. 오류 수정 검증 (리스트 형태가 아니거나, 빈 리스트인 경우)
                    if (not isinstance(corrected_sql, list)) or (len(corrected_sql)) < 1:
                        # ❌ AI가 제대로 된 SQL을 생성하지 못함
                        print(f"{self.sql_id}: Not a valid SQL: {corrected_sql}")
                        continue # 다음 시도로 넘어감

                    # 2-5-3. 정상적인 SQL 생성 시 실행
                    corrected_sql = max(corrected_sql, key=len) # 가장 긴 SQL 선택
                    
                    # 2-5-4. 오류 수정 결과 실행
                    results = self.sql_env.execute_sql_api(corrected_sql, self.sql_id, api=self.api, max_len=self.csv_max_len, sqlite_path=self.sqlite_path)
                    logger.info("[수정된 SQL 결과]\n"+str(results)+"\n[수정된 SQL 결과]")
                    max_try -= 1
                    simplify = False

                # 2-5-5. 오류 수정 성공 시 결과 저장
                if isinstance(results, str) and results != self.empty_result:
                    error_rec.append(1) # 오류 수정 성공 시 1로 저장

                    # 2-5-6. 오류 수정 성공 시 다른 SQL들도 수정 시도
                    if sqls != []:
                        response = self.chat_session_pre.get_model_response(self.prompt_class.get_exploration_refine_prompt(sql, corrected_sql, sqls), "sql")

                        if isinstance(response, list) and response != []:
                            response_sqls = []
                            for s in response:
                                try:
                                    queries = split_sql_safe(s)
                                    response_sqls += queries
                                except:
                                    pass
                            if len(response_sqls) >= len(sqls) // 2:
                                # 기존 SQL의 절반 이상을 수정했을 때, LLM이 수정 요청을 잘 이해했다고 판단 -> 수정된 SQL로 기존 SQL을 대체
                                sqls = response_sqls
                                logger.info("[다른 SQL들 수정됨]\n"+self.chat_session_pre.messages[-1]['content']+"\n[다른 SQL들 수정됨]")
                else:
                    error_rec.append(0)
                    # Many times error, return
                    if len(error_rec) > 5 and sum(error_rec[-5:]) == 0:
                        return result_dic_list
                    continue
                if not corrected_sql:
                    continue
                result_dic['sql'] = corrected_sql
                result_dic['res'] = results
                # self.chat_session_pre.messages.append({"role": "user", "content": f"Successfully corrected. SQL:\n{corrected_sql}\nResults:\n{results}"})
                logger.info("[성공적으로 수정됨]\n" +  f"성공적으로 실행됨. SQL:\n{sql}\n결과:\n{results}" + "\n[성공적으로 수정됨]")
        return result_dic_list

    def self_correct(self, sql, error, logger, simplify=False):
        """
        '후보 SQL' 오류 수정 메커니즘 (단발성)
        : 오류가 있는 SQL을 수정하고, 수정된 SQL을 반환합니다. (GPT 모델을 통해 수정)

        Args:
            sql: 오류가 있는 SQL
            error: 오류 메시지
            logger: 로깅 객체
            simplify: 단순화 여부

        Returns:
            response: 수정된 SQL
        """

        # 1. 오류 메시지를 받아서, GPT에게 수정 요청
        prompt = self.prompt_class.get_exploration_self_correct_prompt(sql, error)
        
        # 2. 단순화 옵션 (결과가 비어있을 때)
        if simplify:
            prompt += "Since the output is empty, please simplify some conditions of the past sql.\n"
        
        # 3. 1회 수정 후 즉시 반환
        response = self.chat_session_pre.get_model_response(prompt, "sql")

        max_try = self.max_try
        while max_try > 0 and (not isinstance(response, str) or len(response) > 1):
            response = self.chat_session_pre.get_model_response("Please generate only one SQL with thinking process.", "sql")
            max_try -= 1
        logger.info("[수정된 SQL]\n" + self.chat_session_pre.messages[-1]['content'] + "\n[수정된 SQL]")
        return response

    def format_answer(self, task, chat_session: Type[GPTChat]):
        format_prompt = self.prompt_class.get_format_prompt()
        response_csv = chat_session.get_model_response("Task: " + task + format_prompt, "csv")
        response_csv = "```csv\n"+response_csv[0].split("\n")[0]+"\n```"
        return response_csv

    def exploration(self, task, table_struct, table_info, logger):
        """
        Column Exploration을 수행합니다.

        Args:
            task: 작업 설명
            table_struct: 테이블 구조 정보
            table_info: 테이블 정보
            logger: 로깅 객체

        Returns:
            pre_info: 준비 정보
            response_pre_txt: 응답 텍스트
            max_try: 최대 시도 횟수
        """
        pre_info = ''
        task = table_info + "\nTask: " + task + "\n"
        max_try = self.max_try
        while max_try > 0:
            exploration_prompt = task + self.prompt_class.get_exploration_prompt(self.api, table_struct)

            response_pre = self.chat_session_pre.get_model_response(exploration_prompt, "sql")
            response_pre_txt = self.chat_session_pre.messages[-1]['content']
            logger.info("[컬럼 탐색]\n" + response_pre_txt + "\n[컬럼 탐색]")
            if not isinstance(response_pre, list):
                max_try -= 1
                continue
            
            if len(response_pre) == 1:
                response_pre = split_sql_safe(response_pre[0])
            if len(response_pre) < 3:
                max_try -= 1
                print(f"{self.sql_id}: Few sqls, retry preparation.")
                continue
            results_pre_dic_list = self.execute_sqls(response_pre, logger)
            sql_count = 0
            for dic in results_pre_dic_list:
                pre_info += "Query:\n" + dic['sql'] + "\nAnswer:\n" + str(dic['res'])
                if isinstance(dic['res'], str):
                    sql_count += 1

            if sql_count == 0:
                print(f"{self.sql_id}: sql_count: {sql_count}, len(response_pre): {len(response_pre)}. Inadequate preparation, break.")
                max_try = 0
                break

            if len(pre_info) < 1e5:
                break
            print(f"{self.sql_id}: Too long, retry preparation.")
            pre_info = ''
            max_try -= 1

        return pre_info, response_pre_txt, max_try

    def self_refine(self, args, logger, question, format_csv, table_struct, table_info, response_pre_txt, pre_info, csv_save_path, sql_save_path, task=None):
        """
        '최종 답안' SQL의 반복적 개선 단계 (단발성 X)
        
        이 함수는 SQL 쿼리를 반복적으로 개선하여 최적의 결과를 도출합니다.
        주요 기능:
        1) SQL 생성 및 실행
        2) 결과 검증 및 오류 수정
        3) Self-consistency 체크를 통한 결과 안정성 확보
        4) 반복적 개선을 통한 품질 향상

        Args:
            args: 인자 객체 (max_iter, do_self_consistency, early_stop, save_all_results, omnisql_format_pth)
            logger: 로깅 객체
            question: 유저 질문
            format_csv: 답변 형식 지침 (CSV)
            table_struct: 테이블 구조 정보
            table_info: 테이블 정보
            response_pre_txt: 응답 텍스트
            pre_info: Column Exploration 결과
            csv_save_path: CSV 저장 경로
            sql_save_path: SQL 저장 경로
            task: 태스크 정보 (선택적)
        """

        # ==================== SECTION 1: 초기화 ====================
        # 반복 카운터 및 결과 추적을 위한 변수들 초기화
        itercount = 0                    # 현재 반복 횟수
        results_values = []              # 이전 결과값들을 저장 (중복 결과 검출용)
        results_tables = []              # 이전 결과 테이블들을 저장
        error_rec = []                   # 오류 이력 추적 (조기 종료 판단용)

        # 초기 Self-refine 프롬프트 생성
        # 테이블 정보, 태스크, 탐색 결과 등을 종합하여 첫 번째 프롬프트 구성
        self_refine_prompt = self.prompt_class.get_self_refine_prompt(
            table_info, task, pre_info, question, self.api, format_csv, table_struct, args.omnisql_format_pth
        )

        # ==================== SECTION 2: 반복적 개선 메인 루프 ====================
        while itercount < args.max_iter:
            logger.info(f"itercount: {itercount}")
            logger.info("[Self-refine]\n" + self_refine_prompt + "\n[Self-refine]")
            
            # ---------- SUBSECTION 2.1: SQL 생성 단계 ----------
            max_try = self.max_try
            while max_try > 0:
                # GPT 모델로부터 SQL 쿼리 생성 요청
                response = self.chat_session.get_model_response(self_refine_prompt, "sql")

                # 응답 검증: 정확히 하나의 SQL만 생성되었는지 확인
                if not isinstance(response, list) or len(response) != 1:
                    # 잘못된 응답 형태일 경우 재시도 요청
                    self_refine_prompt = "SQL을 하나만 출력하세요."
                else:
                    # 올바른 응답 형태일 경우 루프 탈출
                    break
                max_try -= 1

            # max_try 횟수 초과 시 처리
            if not isinstance(response, list) or response == []:
                # CSV 파일이 존재하면 삭제 (불완전한 결과 방지)
                if os.path.exists(csv_save_path):
                    os.remove(csv_save_path)
                print(f"{self.sql_id}: Error when generating final SQL.")
                break

            logger.info("[Try to run SQL in self-refine]\n" +self.chat_session.messages[-1]['content'] + "\n[Try to run SQL in self-refine]")

            # ---------- SUBSECTION 2.2: SQL 실행 및 결과 검증 ----------
            response = response[0]  # 리스트에서 첫 번째 SQL 추출

            # SQL 실행: API를 통해 실제 데이터베이스에서 쿼리 실행
            executed_result = self.sql_env.execute_sql_api(
                response, self.sql_id, csv_save_path, api=self.api, sqlite_path=self.sqlite_path
            )
            error_rec.append(str(executed_result))  # 실행 결과를 오류 기록에 추가

            # ---------- SUBSECTION 2.3: 조기 종료 조건 체크 ----------
            if args.early_stop and len(error_rec) > 3:
                # 최근 4번의 시도가 모두 동일한 빈 결과를 반환한 경우
                # 더 이상 개선될 가능성이 낮다고 판단하여 조기 종료
                if len(set(error_rec[-4:])) == 1 and error_rec[-1] == self.empty_result:
                    logger.info("No data found for the specified query, remove file.")                    
                    if os.path.exists(csv_save_path):
                        os.remove(csv_save_path)
                    break
            
            # ---------- SUBSECTION 2.4: 성공적 실행 결과 처리 ----------
            if executed_result == '0':  # '0'은 성공적 실행을 의미
                # Self-consistency가 비활성화된 경우: 즉시 결과 저장 후 종료
                if not args.do_self_consistency:
                    with open(sql_save_path, "w", encoding='utf-8') as f:
                        f.write(response)
                    break
                
                # Self-consistency 활성화된 경우: 결과 일관성 검증 시작
                self_consistency_prompt = self.prompt_class.get_self_consistency_prompt(question, format_csv)

                # CSV 결과 데이터 읽기 및 로깅
                with open(csv_save_path) as f:
                    csv_data = f.readlines()
                    csv_data_str = ''.join(csv_data)
                logger.info(f"[Executed results in self-refine]\n{hard_cut(csv_data_str, self.csv_max_len)}\n[Executed results in self-refine]")

                # self_consistency_prompt에 현재 결과 정보 추가
                self_consistency_prompt += "Current answer: \n" + hard_cut(csv_data_str, self.csv_max_len)
                self_consistency_prompt += f"Current SQL:\n{response}"
                
                # 특수 문자 처리: CSV에 따옴표가 포함된 경우 처리 지침 추가
                if '"""' in csv_data_str:
                    self_consistency_prompt += 'Please remove """ in results. Use CAST: CAST(column_name AS STRING).\n'

                # ---------- SUBSECTION 2.5: 결과 데이터 품질 검증 ----------
                csv_buffer = StringIO(csv_data_str)
                df_csv = pd.read_csv(csv_buffer).fillna("")  # 빈 값을 빈 문자열로 처리

                # 중첩된 값 검출: 줄바꿈이 포함된 문자열 값들을 찾아냄
                nested_val = [(item) for i, row in enumerate(df_csv.values.tolist()) 
                             for j, item in enumerate(row) 
                             if isinstance(item, str) and '\n' in item in item]
                
                # 데이터 정규화: 소수점 반올림 및 정렬
                df_csv_copy = df_csv.copy()
                for col in df_csv.select_dtypes(include=['float']):
                    df_csv_copy[col] = df_csv[col].round(2)  # 소수점 2자리로 반올림
                
                sort_col = df_csv_copy.columns[0]
                df_csv_copy_sorted = df_csv_copy[sort_col].astype(str)
                csv_data_str_round2 = df_csv_copy_sorted.to_string()
                df_csv_str = df_csv.astype(str)
                
                # ---------- SUBSECTION 2.6: 결과 중복성 및 일관성 검사 ----------
                if get_values_from_table(csv_data_str_round2) not in results_values:
                    # 새로운 결과인 경우: 데이터 품질 검증
                    if nested_val:
                        # 중첩된 값이 있는 경우: 수정 요청
                        self_consistency_prompt += f"값 {nested_val}이 중첩되어 있습니다. 수정해주세요. 예: '[\nA,\n B\n]'를 'A, B'로 변환.\n"
                    elif not ((df_csv_str == "0") | (df_csv_str == "")).all().any():
                        # 유효한 데이터가 있는 경우: 결과 저장
                        results_values.append(get_values_from_table(csv_data_str_round2))
                        results_tables.append(csv_data_str)
                    else:
                        # 빈 컬럼이 있는 경우: 수정 요청
                        empty_columns = df_csv_str.columns[((df_csv_str == "0") | (df_csv_str == "")).all()].to_list()
                        self_consistency_prompt += f"Empty results in Column {empty_columns}. Please correct them.\n"
                else:
                    # 이전과 동일한 결과인 경우: Self-consistency 달성으로 판단하여 종료
                    logger.info(f"[Consistent results]\n{hard_cut(csv_data_str, 500)}\n[Consistent results]")
                    with open(sql_save_path, "w", encoding='utf-8') as f:
                        f.write(response)
                    break
                
                # ---------- SUBSECTION 2.7: 추가 검증 및 프롬프트 업데이트 ----------
                # SQL에 테이블 생략 키워드가 포함된 경우 모든 테이블 명시 지침 추가
                if any(keyword in response for keyword in self.prompt_class.get_condition_onmit_tables()):
                    self_consistency_prompt += self.prompt_class.get_prompt_dialect_list_all_tables(table_struct, self.api)
                
                # 모든 결과 저장 옵션이 활성화된 경우 파일명 수정
                if args.save_all_results:
                    save_path = save_path[:-4] + str(itercount) + save_path[-4:]
                
                # 다음 반복을 위해 프롬프트 업데이트
                self_refine_prompt = self_consistency_prompt
            
            else:
                # ---------- SUBSECTION 2.8: 실행 오류 처리 ----------
                # SQL 실행 중 오류가 발생한 경우: 오류 정보를 포함한 수정 요청 프롬프트 생성
                self_refine_prompt = f"입력 SQL:\n{response}\n오류 정보:\n" + str(executed_result) + "\n수정하여 완전한 SQL 쿼리 1개만 출력하세요."

            # 반복 카운터 증가
            itercount += 1

        # ==================== SECTION 3: 종료 처리 ====================
        logger.info(f"Total iteration counts: {itercount}")
        
        # 최대 반복 횟수에 도달했지만 만족스러운 결과를 얻지 못한 경우
        if itercount == args.max_iter and not args.save_all_results:
            if os.path.exists(csv_save_path):
                os.remove(csv_save_path)  # 불완전한 결과 파일 삭제
            logger.info("Max Iter, remove file")
        
        # 세션 메시지 길이 로깅 (디버깅 및 모니터링 목적)
        print(f"{self.sql_id}: chat_session len: {self.chat_session.get_message_len()}")

    def gen(self, args, logger, question, format_csv, table_struct, table_info, response_pre_txt, pre_info, csv_save_path, sql_save_path, task=None):
        """
        self_refine 하지 않고, 한 번의 SQL 생성으로 후보 SQL 생성
        """
        gen_prompt = self.prompt_class.get_self_refine_prompt(table_info, task, pre_info, question, self.api, format_csv, table_struct, args.omnisql_format_pth)
        logger.info("[Gen]\n" + gen_prompt + "\n[Gen]")
        max_try = self.max_try
        while max_try > 0:
            response = self.chat_session.get_model_response(gen_prompt, "sql")
            if not isinstance(response, list) or len(response) != 1:
                gen_prompt = "SQL을 하나만 출력하세요."
            else:
                break
            max_try -= 1
        if not isinstance(response, list) or response == []:
            if os.path.exists(csv_save_path):
                os.remove(csv_save_path)
            print(f"{self.sql_id}: Error when generating final SQL.")
        logger.info("[Gen SQL]\n" +self.chat_session.messages[-1]['content'] + "\n[Gen SQL]")
        response = response[0]
        executed_result = self.sql_env.execute_sql_api(response, self.sql_id, csv_save_path, api=self.api, sqlite_path=self.sqlite_path)
        if executed_result == '0':
            with open(sql_save_path, "w", encoding='utf-8') as f:
                f.write(response)

    def model_vote(self, result, sql_paths, search_directory, args, table_info, task):
        """
        AI 모델을 판정관으로 사용한 투표 시스템
        
        동점 상황이나 모든 결과가 다른 상황에서 AI 모델이 SQL들을 직접 평가하여 
        가장 적절한 답안을 선택하는 함수입니다.
        
        동작 과정:
        1. 동점인 SQL들의 내용과 실행 결과를 수집
        2. AI 모델에게 모든 정보를 제공하고 판정 요청
        3. AI가 선택한 SQL을 최종 결과로 저장
        
        Args:
            result: 동점 상황의 투표 결과 딕셔너리 (예: {"0result.sql": 2, "1result.sql": 2})
            sql_paths: SQL 파일명 → CSV 파일명 매핑 (예: {"0result.sql": "0result.csv"})
            search_directory: 후보 파일들이 저장된 디렉토리 경로
            args: 실행 인자 (model_vote 모델 정보 포함)
            table_info: 데이터베이스 테이블 정보
            task: 사용자의 원래 질문/작업
        """
        
        # ==================== SECTION 1: AI 판정관 초기화 ====================
        # AI 모델을 판정관으로 사용하기 위한 새로운 채팅 세션 생성
        chat_session = GPTChat(args.azure, args.model_vote)
        
        # ==================== SECTION 2: 동점 후보들 식별 ====================
        max_value = max(result.values())  # 최고 득표수 찾기 (예: 2표)
        max_dict = {k: v for k, v in result.items() if v == max_value}  # 최고 득표수를 받은 후보들만 추출
        # 예: {"0result.sql": 2, "2result.sql": 2} (1result.sql은 1표라서 제외)
        
        # ==================== SECTION 3: AI 판정용 프롬프트 구성 ====================
        # 3-1. 기본 프롬프트 헤더 생성
        prompt = f"데이터베이스 정보, 작업 및 후보 SQL들과 그 결과가 제공됩니다. 데이터베이스 정보를 바탕으로 가장 올바른 것을 선택해주세요:\n{table_info}. 작업: {task}. 다음은 후보 SQL들과 답변들입니다: \n"
        
        # 3-2. 각 동점 후보의 상세 정보 추가
        for sql, counts in max_dict.items():  # 동점인 후보들만 순회
            sql_path = os.path.join(search_directory, sql)        # SQL 파일 전체 경로
            csv_path = os.path.join(search_directory, sql_paths[sql])  # 결과 CSV 파일 전체 경로

            # 파일 존재 여부 확인 후 내용 추가
            if os.path.exists(sql_path) and os.path.exists(csv_path):
                # SQL 파일 내용 추가
                prompt += "SQL 파일명: " + sql + "\n"
                with open(sql_path) as f:
                    prompt += f.read()  # SQL 쿼리 전체 내용
                
                # CSV 결과 파일 내용 추가 (5000자로 제한)
                prompt += "CSV 파일명: " + sql_paths[sql] + "\n"
                with open(csv_path) as f:
                    prompt += hard_cut(f.read(), 5000)  # 결과 데이터 (용량 제한)

        # ==================== SECTION 4: AI 판정 지침 추가 ====================
        max_try = 3  # 최대 재시도 횟수
        
        # 4-1. 판정 요청 및 출력 형식 지정
        prompt += "각 답변의 SQL과 결과를 비교하고, 단계별로 생각해서 하나의 SQL을 정답으로 선택하세요. 사고 과정과 함께 sql 이름을 ```plaintext\nxxx.sql``` 형식으로 출력하세요. 'plaintext'를 무시하지 마세요.\n"
        
        # 4-2. 판정 기준 제시
        prompt += "null 값이나 0 값이 있는 결과는 틀린 답변일 가능성이 높습니다.\n"
        prompt += "추론 단계는 다음과 같아야 합니다: 1. 비합리적인 결과 제외하기. 2. 결과가 작업 설명과 일치하는지 확인하기. 3. SQL이 작업 설명과 일치하는지 분석하기.\n"
        
        # ==================== SECTION 5: AI 모델에게 판정 요청 ====================
        response = chat_session.get_model_response(prompt, "plaintext")
        
        # ==================== SECTION 6: AI 응답 검증 및 재시도 ====================
        while max_try > 0:
            # 응답이 올바른 형태인지 확인: 리스트이고, ".sql"이 포함되어야 함
            if not response or not isinstance(response, list) or ".sql" not in response[0]:
                print(f"{search_directory}, 남은 max_try: {max_try}, {response}")
                # 잘못된 응답 시 재요청
                response = chat_session.get_model_response("sql 이름을 ```plaintext\nxxx.sql``` 형식으로 출력하세요. 'plaintext'를 무시하지 마세요.", "plaintext")
            else:
                # 올바른 응답 형태일 경우 반복 종료
                break
            max_try -= 1
        
        # ==================== SECTION 7: 응답 실패 처리 ====================
        if max_try == 0:
            print(f"{search_directory} 비어있음")
            return  # AI가 올바른 형태로 응답하지 못함 → 판정 포기
        
        # ==================== SECTION 8: 선택된 SQL 검증 및 최종 저장 ====================
        # 8-1. AI가 선택한 SQL 파일 읽기
        with open(os.path.join(search_directory, response[0].strip())) as f:
            selected_sql = f.read()  # 선택된 SQL 쿼리 내용
        
        # 8-2. 선택된 SQL 실행 검증
        sql_env = SqlEnv()
        # SQL이 정상적으로 실행되는지 확인 ('0'은 성공을 의미)
        if sql_env.execute_sql_api(selected_sql, self.sql_id, self.complete_csv_save_path, api=self.api, sqlite_path=self.sqlite_path) == '0':
            # 8-3. 검증 성공 시 최종 결과 파일들 저장
            # SQL 파일 저장
            with open(self.complete_sql_save_path, "w", encoding='utf-8') as f:
                f.write(selected_sql)
            
            # 투표 로그 저장 (AI의 판정 과정 기록)
            with open(self.complete_vote_log_path, "w", encoding='utf-8') as f:
                f.write("[투표]\n"+prompt+"\n[투표]")  # 판정 요청 프롬프트
                f.write(chat_session.messages[-1]['content'])  # AI의 판정 결과
        
        # 8-4. 정리
        sql_env.close_db()  # 데이터베이스 연결 종료

    def vote_result(self, search_directory, args, sql_paths, table_info, task):
        """
        최종 답안 선택 단계: 여러 후보 SQL 중 다수결 투표로 최종 답안 선택
        
        동작 원리:
        1. 각 후보 SQL의 실행 결과(CSV)를 서로 비교
        2. 동일한 결과를 가진 후보들끼리 서로에게 투표
        3. 가장 많은 투표를 받은 후보를 최종 답안으로 선택
        4. 동점일 경우 추가 처리 로직 적용 (AI 판정, 랜덤 선택 등)

        Args:
            search_directory: 후보 파일들이 저장된 디렉토리 경로
            args: 실행 인자 (model_vote, final_choose, random_vote_for_tie 등)
            sql_paths: SQL 파일명 → CSV 파일명 매핑 딕셔너리 (예: {"0result.sql": "0result.csv"})
            table_info: 데이터베이스 테이블 정보
            task: 사용자의 원래 질문/작업
        """
        
        # ==================== SECTION 1: 초기 변수 설정 ====================
        result = {}           # 최종 투표 결과: {SQL파일명: 투표수} (예: {"0result.sql": 2})
        result_name = {}      # 동일한 결과를 가진 CSV 파일들의 매핑 정보
        result_all = {}       # 모든 후보의 투표 수 (0표 포함): {SQL파일명: 투표수}
        all_values = []       # 실제로 존재하는 모든 CSV 파일의 전체 경로 리스트

        # ==================== SECTION 2: 존재하는 CSV 파일 수집 ====================
        # sql_paths.values()는 CSV 파일명들 (예: ["0result.csv", "1result.csv", "2result.csv"])
        for v in sql_paths.values():
            csv_file_path = os.path.join(search_directory, v)
            if os.path.exists(csv_file_path):
                all_values.append(csv_file_path)

        # ==================== SECTION 3: 투표 집계 (핵심 로직) ====================
        if len(all_values) > 1:  # CSV 파일이 2개 이상일 때만 비교 가능
            # 각 후보(SQL-CSV 쌍)에 대해 반복
            for key, value in sql_paths.items():  # key: SQL파일명, value: CSV파일명
                complete_value = os.path.join(search_directory, value)  # 현재 후보의 CSV 파일 전체 경로
                
                if os.path.exists(complete_value):
                    same_ans = 0  # 현재 후보와 동일한 결과를 가진 **다른** 후보의 수
                    
                    # 현재 후보를 모든 다른 후보들과 비교
                    for v in all_values:
                        # CSV 파일 읽기 (인코딩 오류 대응)
                        try:
                            v_df = pd.read_csv(v, encoding='utf-8')                    # 비교 대상 CSV
                            c_df = pd.read_csv(complete_value, encoding='utf-8')       # 현재 후보 CSV
                        except UnicodeDecodeError:
                            try:
                                v_df = pd.read_csv(v, encoding='cp949')
                                c_df = pd.read_csv(complete_value, encoding='cp949')
                            except UnicodeDecodeError:
                                v_df = pd.read_csv(v, encoding='latin-1')
                                c_df = pd.read_csv(complete_value, encoding='latin-1')
                        
                        # 투표 조건 검사: 4가지 조건을 모두 만족해야 투표
                        if (v != complete_value and                                    # 1) 자기 자신이 아님
                            is_valid_result(v_df) and                                 # 2) 비교 대상이 유효한 결과
                            compare_pandas_table(v_df, c_df, ignore_order=True) and   # 3) 내용이 동일함 (순서 무시)
                            v_df.shape == c_df.shape):                               # 4) 크기(행수, 열수)가 동일함
                            
                            same_ans += 1  # 동일한 결과를 가진 다른 후보 발견 → 투표 +1
                            result_name[v] = result_name.get(v, []) + [complete_value]  # 매핑 정보 기록
                    
                    result_all[key] = same_ans  # 각 SQL 후보가 받은 투표 수 기록

            # ==================== SECTION 4: 투표 결과 정리 ====================
            result_name = filter_bijection_like_dict(result_name)  # 중복 제거 및 양방향 매핑 정리
            
            # CSV 파일명을 SQL 파일명으로 변환하여 최종 투표 결과 생성
            for key, value in result_name.items():
                sql_filename = key.split("/")[-1].replace(".csv", ".sql")  # 경로에서 파일명만 추출 후 확장자 변경
                result[sql_filename] = len(value)  # 투표 수 = 동일한 결과를 가진 후보들의 수

        # ==================== SECTION 5: 투표 결과가 없는 경우 처리 ====================
        if not result:  # 아무도 투표를 받지 못한 경우 (모든 결과가 서로 다름)
            if not result_all:
                # 아예 후보가 없거나 비교할 수 없는 상황
                print(f"{search_directory} empty results")
                return
            elif args.model_vote:
                # 모든 후보가 0표 → AI 모델을 판정관으로 사용
                assert all(v == 0 for k, v in result_all.items()), result  # 모든 후보가 정말 0표인지 확인
                result_all = {k: v + 1 for k, v in result_all.items()}     # 모든 후보에 1표씩 추가하여 동점 만들기
                self.model_vote(result_all, sql_paths, search_directory, args, table_info, task)
            elif args.final_choose:
                # 첫 번째 후보를 임의로 선택
                first_key = next(iter(result_all))
                shutil.copy2(os.path.join(search_directory, first_key), self.complete_sql_save_path)
                shutil.copy2(os.path.join(search_directory, sql_paths[first_key]), self.complete_csv_save_path) 
                shutil.copy2(os.path.join(search_directory, first_key.replace(self.sql_save_name, self.log_save_name)), self.complete_log_save_path)               
            else:
                # 아무 처리도 하지 않고 종료
                print(f"{search_directory} Empty, return")
            return

        # ==================== SECTION 6: 정상 투표 결과 처리 ====================
        # 투표 수 기준으로 내림차순 정렬 (가장 많은 투표를 받은 후보가 첫 번째)
        sorted_dict = dict(sorted(result.items(), key=lambda item: item[1], reverse=True))
        first_key = next(iter(sorted_dict))  # 가장 많은 투표를 받은 SQL 파일명

        # ==================== SECTION 7: 동점 여부 확인 ====================
        vote_counts = list(sorted_dict.values())        # 모든 투표 수 리스트 (예: [3, 2, 2, 1])
        max_vote = max(vote_counts)                      # 최고 득표수 (예: 3)
        num_with_max_vote = vote_counts.count(max_vote)  # 최고 득표수를 받은 후보의 수 (예: 1)
        has_tie = num_with_max_vote > (max_vote + 1)     # 동점 여부 판정 (복잡한 동점 상황 감지)
        
        if has_tie:  # 동점인 경우
            assert num_with_max_vote % (max_vote + 1) == 0, result_name  # 동점 상황 검증
            if args.model_vote:
                # AI 모델을 판정관으로 사용하여 동점 해결
                self.model_vote(result, sql_paths, search_directory, args, table_info, task)
                return
            if not args.random_vote_for_tie:
                # 동점 상황에서 랜덤 선택을 허용하지 않으면 포기
                print(f"{search_directory} has_tie {sorted_dict}, return")
                return

        # ==================== SECTION 8: 최종 승자를 결과 파일로 복사 ====================
        # 승리한 SQL 파일을 최종 결과 파일 이름으로 복사
        shutil.copy2(os.path.join(search_directory, first_key), self.complete_sql_save_path)
        # 해당 SQL의 실행 결과 CSV도 복사
        shutil.copy2(os.path.join(search_directory, sql_paths[first_key]), self.complete_csv_save_path)
        # 해당 SQL의 로그 파일도 복사
        shutil.copy2(os.path.join(search_directory, first_key.replace(self.sql_save_name, self.log_save_name)), self.complete_log_save_path)