#!/usr/bin/env python3
"""
Snowflake 연결 테스트 스크립트
"""
import json
import snowflake.connector
import sys
import os

def test_snowflake_connection():
    """Snowflake 연결을 테스트합니다."""
    
    print("🔍 Snowflake 연결 테스트를 시작합니다...")
    
    # credential 파일 읽기
    try:
        with open('snowflake_credential.json', 'r') as f:
            credentials = json.load(f)
        print("✅ credential 파일을 성공적으로 읽었습니다.")
    except FileNotFoundError:
        print("❌ snowflake_credential.json 파일을 찾을 수 없습니다.")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파일 형식 오류: {e}")
        return False
    
    # credential 정보 확인 (비밀번호는 마스킹)
    print(f"📋 연결 정보:")
    print(f"   Account: {credentials.get('account', 'N/A')}")
    print(f"   User: {credentials.get('user', 'N/A')}")
    print(f"   Database: {credentials.get('database', 'N/A')}")
    print(f"   Schema: {credentials.get('schema', 'N/A')}")
    print(f"   Warehouse: {credentials.get('warehouse', 'N/A')}")
    print(f"   Password: {'*' * len(credentials.get('password', ''))}")
    
    # 연결 시도
    try:
        print("\n🔌 Snowflake에 연결 중...")
        conn = snowflake.connector.connect(**credentials)
        print("✅ Snowflake 연결 성공!")
        
        # 기본 쿼리 테스트
        print("\n🧪 기본 쿼리 테스트 중...")
        cursor = conn.cursor()
        
        # 테스트 1: SELECT 1
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        print(f"✅ SELECT 1 테스트: {result[0]}")
        
        # 테스트 2: 현재 날짜/시간
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        result = cursor.fetchone()
        print(f"✅ 현재 시간: {result[0]}")
        
        # 테스트 3: 데이터베이스 정보
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        print(f"✅ 현재 컨텍스트:")
        print(f"   Database: {result[0]}")
        print(f"   Schema: {result[1]}")
        print(f"   Warehouse: {result[2]}")
        
        # 테스트 4: 스키마의 테이블 목록 (전체)
        try:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            if tables:
                print(f"✅ 스키마 내 전체 테이블 ({len(tables)}개):")
                for i, table in enumerate(tables, 1):
                    print(f"   {i:2d}. {table[1]}")  # table[1]이 테이블 이름
            else:
                print("ℹ️  현재 스키마에 테이블이 없습니다.")
        except Exception as e:
            print(f"⚠️  테이블 목록 조회 실패: {e}")
        
        cursor.close()
        conn.close()
        
        print("\n🎉 모든 테스트가 성공적으로 완료되었습니다!")
        return True
        
    except snowflake.connector.errors.DatabaseError as e:
        print(f"❌ 데이터베이스 연결 오류: {e}")
        return False
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"❌ 프로그래밍 오류: {e}")
        return False
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        return False

if __name__ == "__main__":
    success = test_snowflake_connection()
    sys.exit(0 if success else 1) 