import os
import json
import shutil
import zipfile
import argparse

# 아마 이 부분을 내 DB에 맞게 수정해야 할 것 같음
JSONL_PATH = '../../spider2-fnf/spider2-fnf.jsonl'
DATABASE_PATH = '../../spider2-fnf/resource/databases/'
DOCUMENT_PATH = '../../spider2-fnf/resource/documents'


def clear_folder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        os.makedirs(folder_path)
    else:
        print(f"The folder {folder_path} does not exist.")



def setup_snowflake():
    credential_path = 'snowflake_credential.json'
    with open(JSONL_PATH, "r") as f:
        examples = [json.loads(line) for line in f]
    for example in examples:
        instance_id = example['instance_id']
        folder_path = f'{args.example_folder}/{instance_id}'
        target_credential_path = os.path.join(folder_path, 'snowflake_credential.json')

        if os.path.exists(target_credential_path):
            os.remove(target_credential_path)

        shutil.copy(credential_path, target_credential_path)
        print(f"인스턴스 {instance_id}에 대한 Snowflake 자격 증명을 복사했습니다.")
    
    print("Snowflake 설정 완료...")



    
error_dbs = []
def setup_add_schema(args):

    with open(JSONL_PATH, "r") as f:
        examples = [json.loads(line) for line in f]            
            
    for example in examples:
        instance_id = example['instance_id']
        db_id = example['db_id']
        example_folder = f'{args.example_folder}/{instance_id}'
        assert os.path.exists(example_folder)
        dest_folder = os.path.join(example_folder, db_id)  # Use db_id as the folder name
        if os.path.exists(dest_folder):
            shutil.rmtree(dest_folder)
        os.makedirs(dest_folder)
        src_folder = os.path.join(DATABASE_PATH, db_id)
        shutil.copytree(src_folder, dest_folder, dirs_exist_ok=True)

        






def add_snowflake_agent_setting():

    with open(JSONL_PATH, "r") as f:
        examples = [json.loads(line) for line in f]

    snowflake_agent_dir_path = os.path.join('./',args.example_folder)
    
    clear_folder(snowflake_agent_dir_path) # 기존 폴더가 존재하면 삭제 후 재생성

    if not os.path.exists(snowflake_agent_dir_path):
        os.makedirs(snowflake_agent_dir_path)
    shutil.copy(JSONL_PATH, snowflake_agent_dir_path) # JSONL 파일 복사


    for example in examples:
        instance_id = example['instance_id']
        example_path = os.path.join(snowflake_agent_dir_path, f"{instance_id}") # 인스턴스 폴더 생성
        if not os.path.exists(example_path):
            os.makedirs(example_path)
        external_knowledge = example['external_knowledge'] # 외부 지식 파일 복사
        if external_knowledge != None:
            shutil.copy(os.path.join(DOCUMENT_PATH, external_knowledge), example_path)





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Spider 2.0용 설정")
    parser.add_argument("--snowflake", action="store_true", help="Snowflake 설정")
    parser.add_argument("--add_schema", action="store_true", help="스키마 추가")
    parser.add_argument('--example_folder', type=str, default="examples_snow")

    args = parser.parse_args()

    # 1. 새로운 examples_snow 폴더 생성 -> JSONL 안에 있는 instance_id별로 폴더 생성 후 파일 복사 (external_knowledge 파일이 있는 경우 이것도 복사)
    add_snowflake_agent_setting() 

    # 2. Snowflake credential 복사
    setup_snowflake()

    # 3. DB Schema 추가
    setup_add_schema(args)

