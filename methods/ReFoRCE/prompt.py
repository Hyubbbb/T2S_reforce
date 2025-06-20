omni_sql_input_prompt_template = '''작업 개요:
당신은 데이터 사이언스 전문가입니다. 아래에 데이터베이스 스키마와 자연어 질문이 제공됩니다. 스키마를 이해하고 질문에 답하는 유효한 SQL 쿼리를 생성하는 것이 당신의 작업입니다.

데이터베이스 엔진:
{db_engine}

데이터베이스 스키마:
{db_details}
이 스키마는 테이블, 컬럼, 기본 키, 외래 키, 관련 관계 또는 제약 조건을 포함한 데이터베이스의 구조를 설명합니다.

질문:
{question}

지시사항:
- 질문에서 요구하는 정보만 출력하세요. 질문에서 특정 컬럼을 요구하면 SELECT 절에 해당 컬럼만 포함하세요.
- 생성된 쿼리는 질문에서 요구하는 모든 정보를 누락이나 추가 정보 없이 반환해야 합니다.
- 최종 SQL 쿼리를 생성하기 전에 쿼리 작성 단계를 차근차근 생각해주세요.

출력 형식:
답변에서 생성된 SQL 쿼리를 코드 블록으로 묶어주세요:
```sql
-- 당신의 SQL 쿼리
```

차근차근 생각하며 올바른 SQL 쿼리를 찾으세요.
'''

class Prompts:
    def __init__(self):
        pass

    def get_condition_onmit_tables(self):
        """LLM이 자주 사용하는 생략 표현들을 미리 정의"""
        # return ["-- 모든 것 포함", "-- 생략", "-- 계속", "-- 모든 것 통합", "-- ...", "-- 모든 것 나열", "-- 이것을 대체", "-- 각 테이블", "-- 기타 추가"]
        return ["-- Include all", "-- Omit", "-- Continue", "-- Union all", "-- ...", "-- List all", "-- Replace this", "-- Each table", "-- Add other"]

    def get_prompt_dialect_list_all_tables(self, table_struct, api):
        """UNION 연산을 수행할 때 모든 테이블 이름을 명시적으로 나열하는 것을 권장 & 생략 표현 제공"""
        if api == "snowflake":
            return f"여러 테이블에 대해 UNION 연산을 수행할 때는 모든 테이블 이름을 명시적으로 나열하세요. 먼저 Union을 수행한 후 조건과 선택을 추가하세요. 예: SELECT \"col1\", \"col2\" FROM (TABLE1 UNION ALL TABLE2) WHERE ...; 다음과 같이 작성하지 마세요: (SELECT col1, col2 FROM TABLE1 WHERE ...) UNION ALL (SELECT col1, col2 FROM TABLE2 WHERE ...); {self.get_condition_onmit_tables()}를 사용하여 테이블을 생략하지 마세요. 테이블 이름들: {table_struct}\n"
        elif api == "bigquery":
            return "유사한 접두사를 가진 여러 테이블에 대해 UNION 연산을 수행할 때는 와일드카드 테이블을 사용하여 쿼리를 단순화할 수 있습니다. 예: SELECT col1, col2 FROM `project_id.dataset_id.table_prefix*` WHERE _TABLE_SUFFIX IN ('table1_suffix', 'table2_suffix'); 꼭 필요한 경우가 아니면 테이블을 수동으로 나열하는 것을 피하세요.\n"
        else:
            return ""
    
    # 데이터 품질 관련 함수들
    def get_prompt_fuzzy_query(self): # ?
        return "문자열 매칭 시나리오에서 문자열이 확정된 경우 fuzzy query를 사용하지 마세요. 예: \"book\"이라는 단어가 포함된 객체의 제목 가져오기\n하지만 문자열이 확정되지 않은 경우 fuzzy query를 사용하고 대소문자를 구분하지 마세요. 예: \"교육\"을 언급하는 기사 가져오기.\n"
    
    def get_prompt_decimal_places(self):
        return "작업 설명에서 소수점 자릿수를 지정하지 않은 경우 모든 소수를 네 자리까지 유지하세요.\n"
    
    def get_prompt_convert_symbols(self):
        return "문자열 매칭 시나리오에서 비표준 기호를 '%'로 변환하세요. 예: ('he's를 he%s로)\n"
    
    # 지식 제한 함수
    def get_prompt_knowledge(self):
        return "당신의 지식은 데이터베이스의 정보를 기반으로 합니다. 자신의 지식을 사용하지 마세요.\n"
    
    # 데이터베이스 방언 관련 함수들
    def get_prompt_dialect_nested(self, api):
        if api == "snowflake":
            return "JSON nested format의 컬럼의 경우: 예: SELECT t.\"column_name\", f.value::VARIANT:\"key_name\"::STRING AS \"abstract_text\" FROM PATENTS.PATENTS.PUBLICATIONS t, LATERAL FLATTEN(input => t.\"json_column_name\") f; 작업에 직접 답하지 말고 모든 컬럼 이름이 큰따옴표로 묶여 있는지 확인하세요. event_params와 같은 중첩 컬럼의 구조를 모르는 경우 먼저 전체 컬럼을 확인하세요: SELECT f.value FROM table, LATERAL FLATTEN(input => t.\"event_params\") f;\n"
        elif api == "bigquery":
            return "Nested JSON 컬럼에서 특정 키 추출: SELECT t.\"column_name\", JSON_EXTRACT_SCALAR(f.value, \"$.key_name\") AS \"abstract_text\" FROM `database.schema.table` AS t, UNNEST(JSON_EXTRACT_ARRAY(t.\"json_column_name\")) AS f;\nNested 컬럼(예: event_params)의 구조를 모르는 경우 먼저 전체 컬럼을 검사하세요: SELECT f.value FROM `project.dataset.table` AS t, UNNEST(JSON_EXTRACT_ARRAY(t.\"event_params\")) AS f;\n"
        elif api == "sqlite":
            return "Nested JSON 컬럼에서 특정 키 추출: SELECT t.\"column_name\", json_extract(f.value, '$.key_name') AS \"abstract_text\" FROM \"table_name\" AS t, json_each(t.\"json_column_name\") AS f;\nNested 컬럼(예: event_params)의 구조를 모르는 경우 먼저 전체 컬럼을 검사하세요: SELECT f.value FROM \"table_name\" AS t, json_each(t.\"event_params\") AS f;\n"
        else:
            return "지원되지 않는 API입니다. 유효한 API 이름('snowflake', 'bigquery', 'sqlite')을 제공해주세요."
        
    def get_prompt_dialect_basic(self, api):
        if api == "snowflake":
            return "```sql\nSELECT \"COLUMN_NAME\" FROM DATABASE.SCHEMA.TABLE WHERE ... ``` (\"DATABASE\", \"SCHEMA\", \"TABLE\"을 실제 이름에 맞게 조정하고, 모든 컬럼 이름이 큰따옴표로 묶여 있는지 확인하세요)"
        elif api == "bigquery":
            return "```sql\nSELECT `column_name` FROM `database.schema.table` WHERE ... ``` (`database`, `schema`, `table`을 실제 이름으로 바꾸세요. 컬럼 이름과 테이블 식별자를 백틱으로 묶으세요.)"
        elif api == "sqlite":
            return "```sql\nSELECT DISTINCT \"column_name\" FROM \"table_name\" WHERE ... ``` (\"table_name\"을 실제 테이블 이름으로 바꾸세요. 특수 문자가 포함되거나 예약어와 일치하는 경우 테이블과 컬럼 이름을 큰따옴표로 묶으세요.)"
        else:
            raise NotImplementedError("지원되지 않는 API입니다. 유효한 API 이름('snowflake', 'bigquery', 'sqlite')을 제공해주세요.")
    
    def get_prompt_dialect_string_matching(self, api):
        if api == "snowflake":
            return "확신이 서지 않는 경우 문자열을 직접 매치하지 마세요. 먼저 fuzzy query를 사용하세요: WHERE str ILIKE \"%target_str%\" 문자열 매칭의 경우, 예를 들어 meat lovers, 공백을 %로 바꿔야 합니다. 예: ILIKE %meat%lovers%.\n"
        elif api == "bigquery":
            return "확신이 서지 않는 경우 문자열을 직접 매치하지 마세요. fuzzy query에는 LOWER를 사용하세요: WHERE LOWER(str) LIKE LOWER('%target_str%'). 예를 들어 'meat lovers'를 매치하려면 LOWER(str) LIKE '%meat%lovers%'를 사용하세요.\n"
        elif api == "sqlite":
            return "확신이 서지 않는 경우 문자열을 직접 매치하지 마세요. fuzzy query의 경우: WHERE str LIKE '%target_str%'를 사용하세요. 예를 들어 'meat lovers'를 매치하려면 WHERE str LIKE '%meat%lovers%'를 사용하세요. 대소문자 구분이 필요한 경우 COLLATE BINARY를 추가하세요: WHERE str LIKE '%target_str%' COLLATE BINARY.\n"
        else:
            raise NotImplementedError("지원되지 않는 API입니다. 유효한 API 이름('snowflake', 'bigquery', 'sqlite')을 제공해주세요.")

    def get_format_prompt(self):
        format_prompt = "이것은 SQL 작업입니다. 테이블과 같은 ```csv``` 형식으로 가장 간단한 답변 형식을 제공하세요.\n"
        format_prompt += "예시1. 각 지점에서의 여행 좌표와 누적 여행 거리 포함. 형식: ```csv\ntravel_coordinates,cumulative_travel_distance\nPOINT(longitude1 latitude1),distance1:int\nPOINT(longitude2 latitude2),distance2:int\n...```\n"
        format_prompt += "이름이나 ID를 지정하지 않고 질문할 때는 둘 다 제공하세요. 예시2. 2017년 매월 계절성 조정 판매 비율이 지속적으로 2 이상을 유지한 제품은? 형식: ```csv\nproduct_name,product_id\nproduct_name1:str,product_id1:int\n...```\n"
        format_prompt += "SQL 쿼리를 출력하지 마세요.\n"
        return format_prompt

    def get_exploration_prompt(self, api, table_struct):
        # 10개까지의 탐색용 SQL 생성 지시
        # 각 데이터베이스 방언별 특수 처리 지침
        # 중첩 JSON, 문자열 매칭 등 고급 기능 안내
        
        exploration_prompt = f"최종 답변을 위해 간단한 것부터 복잡한 것까지 최대 10개의 {api} SQL 쿼리를 다음 형식으로 작성하세요:\n {self.get_prompt_dialect_basic(api)}\n```sql``` 코드 블록에서 관련 컬럼의 값들을 이해하기 위해.\n"
        exploration_prompt += "각 쿼리는 다르게 작성해야 합니다. SCHEMA나 데이터 타입 확인에 대한 쿼리는 작성하지 마세요. SELECT 쿼리만 작성할 수 있습니다. DISTINCT를 사용해보세요. 각 SQL은 20행으로 제한하세요.\n"
        exploration_prompt += "각 SQL에 대한 설명을 작성하세요. 형식: ```sql\n--설명: \n```.\n"

        exploration_prompt += self.get_prompt_dialect_nested(api)
                
        exploration_prompt += self.get_prompt_convert_symbols()
        
        exploration_prompt += self.get_prompt_dialect_string_matching(api)
        
        exploration_prompt += "시간 관련 쿼리의 경우 형식이 다양하므로 특정 형식을 확신하지 않는 한 시간 변환 함수 사용을 피하세요.\n"
        
        exploration_prompt += "SQL을 생성할 때 따옴표 매칭에 주의하세요: 'Vegetarian\"; '와 \"를 매치하는 경우가 있어 오류가 발생할 수 있습니다.\n"

        exploration_prompt += f"{table_struct}에 있는 테이블만 사용할 수 있습니다."
        
        exploration_prompt += self.get_prompt_knowledge()

        return exploration_prompt

    def get_exploration_refine_prompt(self, sql, corrected_sql, sqls):
        return f"```sql\n{sql}```이 ```sql\n{corrected_sql}```로 수정되었습니다. 유사한 오류가 있는 다른 SQL들을 수정해주세요. SQL들: {sqls}. 각 SQL에 대해 ```sql\n--설명: \n``` 형식으로 답변하세요.\n"

    def get_exploration_self_correct_prompt(self, sql, error):
        return f"입력 SQL:\n{sql}\n오류 정보:\n" + str(error) + "\n이전 컨텍스트를 바탕으로 수정하고 ```sql\n--설명: \n``` 형식으로 사고 과정과 함께 하나의 SQL 쿼리만 출력하세요. SQL 없이 분석만 하거나 여러 SQL을 출력하지 마세요.\n"

    def get_self_refine_prompt(self, table_info, task, pre_info, question, api, format_csv, table_struct, omnisql_format_pth=None):
        # if omnisql_format_pth:
        #     if task == "lite":
        #         return omni_sql_input_prompt_template.format(
        #             db_engine = "SQLite",
        #             db_details = table_info,
        #             question = question
        #         )
        #     elif task == "BIRD":
        #         return table_info
        refine_prompt = table_info + "\n"
        refine_prompt += "컬럼 탐색 후 몇 가지 few-shot 예시가 도움이 될 수 있습니다:\n" + pre_info if pre_info else ""

        refine_prompt += "작업: " + question + "\n"+f'\n단계별로 생각하고 ```sql``` 형식으로 {api} 방언의 완전한 SQL 하나만 답변하세요.\n'
        refine_prompt += f'SQL 사용 예시: {self.get_prompt_dialect_basic(api)}\n'
        refine_prompt += f"다음과 같은 답변 형식을 따르세요: {format_csv}.\n" if format_csv else ""
        refine_prompt += "답변을 위한 유용한 팁들:\n"
        
        refine_prompt += self.get_prompt_dialect_list_all_tables(table_struct, api)

        if api == "snowflake":
            refine_prompt += "ORDER BY xxx DESC를 사용할 때 null 레코드를 제외하기 위해 NULLS LAST를 추가하세요: ORDER BY xxx DESC NULLS LAST.\n"
        
        # 특정 지침들:
        refine_prompt += "이름이나 ID를 명시하지 않고 질문할 때는 둘 다 반환하세요. 예: 어떤 제품들이 ...? 답변에는 product_name과 product_id가 포함되어야 합니다.\n"
        refine_prompt += "백분율 감소를 묻는 경우 양수 값을 반환해야 합니다. 예: 2021년에 ...와 비교해 몇 퍼센트 포인트 감소했나요? 답변은 감소한 수를 나타내는 양수 값이어야 합니다. ABS()를 사용해보세요.\n"
        refine_prompt += "두 테이블을 묻는 경우 두 테이블을 결합하는 대신 마지막 것으로 답변해야 합니다. 예: 상위 5개 주를 식별하고 ... 전체 4위인 주를 검토하고 그 주의 상위 5개 카운티를 식별하세요. 상위 5개 카운티만 답변해야 합니다.\n"
        if api == "snowflake":
            refine_prompt += "더 정확한 답변을 위해 두 지리적 지점 간의 거리를 계산할 때 ST_DISTANCE를 사용하세요.\n"
        refine_prompt += self.get_prompt_decimal_places() # 소숫점 default 4자리
        
        return refine_prompt

    def get_self_consistency_prompt(self, task, format_csv):
        self_consistency_prompt = f"작업을 다시 검토하여 답변을 확인해주세요:\n {task}\n, 관련 테이블과 컬럼 및 가능한 조건들을 검토한 후 최종 SQL 쿼리를 제공하세요. 다른 쿼리는 출력하지 마세요. 답변이 맞다고 생각하면 현재 SQL을 그대로 출력하세요.\n" 
        self_consistency_prompt += self.get_prompt_decimal_places()
        self_consistency_prompt += f"답변 형식은 다음과 같아야 합니다: {format_csv}\n" if format_csv else ""

        return self_consistency_prompt