import os
import json
import shutil
import zipfile
import argparse

JSONL_PATH = '../../spider2-fnf/spider2-fnf.jsonl'          # JSONL (Instances)
DATABASE_PATH = '../../spider2-fnf/resource/databases/'     # DB Schema (테이블 정보 JSON, DDL.csv)
DOCUMENT_PATH = '../../spider2-fnf/resource/documents'      # External Knowledge (PDF)


def clear_folder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        os.makedirs(folder_path)
    else:
        print(f"The folder {folder_path} does not exist.")



# Step 1: 각 문제 인스턴스별로 폴더 생성 후 파일 복사
def add_snowflake_agent_setting():
    print("=" * 60)
    print("🚀 STEP 1: 기본 폴더 구조 생성 시작")
    print("=" * 60)
    
    # 1. JSONL 파일 읽기
    print(f"📄 JSONL 파일 읽는 중: {JSONL_PATH}")
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        examples = [json.loads(line) for line in f]
    print(f"✅ 총 {len(examples)}개 인스턴스 로드 완료")

    # 2. 폴더 생성
    snowflake_agent_dir_path = os.path.join('./',args.example_folder)
    print(f"📁 메인 폴더 생성: {snowflake_agent_dir_path}")
    clear_folder(snowflake_agent_dir_path) # 기존 폴더가 존재하면 삭제 후 재생성
    print("🗑️  기존 폴더 삭제 후 재생성 완료")

    if not os.path.exists(snowflake_agent_dir_path):
        os.makedirs(snowflake_agent_dir_path)

    # 3. JSONL 파일 복사
    print(f"📋 JSONL 파일 복사: {JSONL_PATH} → {snowflake_agent_dir_path}")
    shutil.copy(JSONL_PATH, snowflake_agent_dir_path)
    print("✅ JSONL 파일 복사 완료")

    # 4. 각 문제 인스턴스별로 폴더 생성 후 파일 복사
    print(f"📂 각 인스턴스별 폴더 생성 시작 ({len(examples)}개)")
    for i, example in enumerate(examples, 1):
        # 4-1. 폴더 생성
        instance_id = example['instance_id'] # 예: 'fnf_001', 'fnf_002'
        example_path = os.path.join(snowflake_agent_dir_path, f"{instance_id}")
        print(f"  [{i:2d}/{len(examples)}] 📁 {instance_id} 폴더 생성 중...")
        
        if not os.path.exists(example_path):
            os.makedirs(example_path) # examples_fnf/fnf_001/ 폴더 생성
        
        # 4-2. 외부 지식 파일 복사 (external_knowledge 파일이 있는 경우)
        external_knowledge = example['external_knowledge'] 
        if external_knowledge != None:
            print(f"      📄 외부 지식 파일 복사: {external_knowledge}")
            shutil.copy(os.path.join(DOCUMENT_PATH, external_knowledge), example_path)
        else:
            print(f"      ❌ 외부 지식 파일 없음")
    
    print("✅ STEP 1 완료: 기본 폴더 구조 생성 완료")
    print()



# Step 2: Snowflake credential 복사
def setup_snowflake():
    print("=" * 60)
    print("🔐 STEP 2: Snowflake 자격 증명 복사 시작")
    print("=" * 60)
    
    credential_path = 'snowflake_credential.json'
    print(f"🔑 자격 증명 파일 경로: {credential_path}")
    
    # 1. JSONL 파일 읽기
    print(f"📄 JSONL 파일 다시 읽는 중: {JSONL_PATH}")
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        examples = [json.loads(line) for line in f]
    print(f"✅ {len(examples)}개 인스턴스 확인")

    # 2. 각 인스턴스별로 Snowflake credential 복사
    print(f"🔐 각 인스턴스에 자격 증명 복사 시작 ({len(examples)}개)")
    for i, example in enumerate(examples, 1):
        instance_id = example['instance_id']
        folder_path = f'{args.example_folder}/{instance_id}'
        target_credential_path = os.path.join(folder_path, 'snowflake_credential.json')

        print(f"  [{i:2d}/{len(examples)}] 🔑 {instance_id} 자격 증명 복사 중...")
        
        if os.path.exists(target_credential_path):
            print(f"      🗑️  기존 자격 증명 파일 삭제")
            os.remove(target_credential_path)

        shutil.copy(credential_path, target_credential_path)
        print(f"      ✅ 자격 증명 복사 완료: {target_credential_path}")
    
    print("✅ STEP 2 완료: Snowflake 설정 완료")
    print()



# Step 3: DB Schema 추가
error_dbs = []
def setup_add_schema(args):
    print("=" * 60)
    print("🗄️  STEP 3: 데이터베이스 스키마 추가 시작")
    print("=" * 60)
    
    # 1. JSONL 파일 읽기
    print(f"📄 JSONL 파일 다시 읽는 중: {JSONL_PATH}")
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        examples = [json.loads(line) for line in f]
    print(f"✅ {len(examples)}개 인스턴스 확인")
    
    # 2. 각 인스턴스별로 DB Schema 복사
    print(f"🗄️  각 인스턴스에 DB 스키마 복사 시작 ({len(examples)}개)")
    for i, example in enumerate(examples, 1):
        instance_id = example['instance_id']
        db_id = example['db_id']
        print(f"  [{i:2d}/{len(examples)}] 🗄️  {instance_id} → DB: {db_id}")

        # 2-1. DB Schema 폴더 생성
        example_folder = f'{args.example_folder}/{instance_id}'
        assert os.path.exists(example_folder), f"인스턴스 폴더가 존재하지 않음: {example_folder}"
        dest_folder = os.path.join(example_folder, db_id)  # Use db_id as the folder name
        
        if os.path.exists(dest_folder):
            print(f"      🗑️  기존 DB 폴더 삭제: {dest_folder}")
            shutil.rmtree(dest_folder)
        print(f"      📁 DB 폴더 생성: {dest_folder}")
        os.makedirs(dest_folder)

        # 2-2. DB Schema 복사 
        src_folder = os.path.join(DATABASE_PATH, db_id)
        print(f"      📋 스키마 복사: {src_folder} → {dest_folder}")
        try:
            # 소스 폴더의 모든 내용을 목적지 폴더로 복사
            if os.path.exists(src_folder):
                for item in os.listdir(src_folder):
                    src_item = os.path.join(src_folder, item)
                    dest_item = os.path.join(dest_folder, item)
                    
                    if os.path.isdir(src_item):
                        shutil.copytree(src_item, dest_item, dirs_exist_ok=True)
                    else:
                        shutil.copy2(src_item, dest_item)
                
                # 복사된 파일 개수 확인 (재귀적으로)
                copied_files = sum([len(files) for r, d, files in os.walk(dest_folder)])
                copied_dirs = sum([len(dirs) for r, dirs, f in os.walk(dest_folder)]) - 1  # 루트 제외
                print(f"      ✅ 스키마 복사 완료 ({copied_files}개 파일, {copied_dirs}개 폴더)")
            else:
                print(f"      ❌ 소스 폴더가 존재하지 않음: {src_folder}")
                error_dbs.append(f"{instance_id}:{db_id}")
        except Exception as e:
            print(f"      ❌ 스키마 복사 실패: {e}")
            error_dbs.append(f"{instance_id}:{db_id}")
    
    if error_dbs:
        print(f"⚠️  스키마 복사 실패한 DB들: {error_dbs}")
    
    print("✅ STEP 3 완료: 데이터베이스 스키마 추가 완료")
    print()








if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Spider 2.0용 설정")
    parser.add_argument("--snowflake", action="store_true", help="Snowflake 설정")
    parser.add_argument("--add_schema", action="store_true", help="스키마 추가")
    parser.add_argument('--example_folder', type=str, default="examples_snow")

    args = parser.parse_args()

    print("🎯 Spider2 FNF 환경 설정을 시작합니다")
    print(f"📂 대상 폴더: {args.example_folder}")
    print(f"📄 JSONL 경로: {JSONL_PATH}")
    print(f"🗄️  DB 경로: {DATABASE_PATH}")
    print(f"📚 문서 경로: {DOCUMENT_PATH}")
    print()

    # 1. 새로운 examples_snow 폴더 생성 -> JSONL 안에 있는 instance_id별로 폴더 생성 후 파일 복사 (external_knowledge 파일이 있는 경우 이것도 복사)
    add_snowflake_agent_setting() 

    # 2. Snowflake credential 복사
    setup_snowflake()

    # 3. DB Schema 추가
    setup_add_schema(args)
    
    print("🎉 모든 설정이 완료되었습니다!")
    print(f"📁 생성된 폴더: {args.example_folder}/")
    print("✅ Spider2 FNF 환경 설정 완료")