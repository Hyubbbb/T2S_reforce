#!/bin/bash
# 실행 명령어:
# ./run_main_no_ce.sh --task fnf --model o3
# Column Exploration을 스킵하는 버전

set -e                            # 에러가 나면 즉시 중단
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")  # 현재 시간을 변수에 저장
AZURE=false                         # AZURE 변수를 기본값 false로 설정
while [[ $# -gt 0 ]]; do            # 인자가 남아있는 동안 반복
  key="$1"
  case $key in
    --azure)
      AZURE=true
      shift # past argument
      ;;
    --task)
      TASK="$2"
      shift
      shift
      ;;
    --model)
      API="$2"
      shift
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# 1. Snowflake Agent 설정 
python spider_agent_setup_${TASK}.py --example_folder examples_${TASK}

# 2. Reconstruct data (Compress data)
python reconstruct_data.py \
    --example_folder examples_${TASK} \
    --add_description \
    --rm_digits \
    --make_folder \
    --clear_long_eg_des

# 3. Prompt.txt 파일 중 200KB 이상인 파일 개수 확인
echo "Number of prompts.txt files in examples_${TASK} larger than 200KB before reducing: $(find examples_${TASK} -type f -name "prompts.txt" -exec du -b {} + | awk '$1 > 200000' | wc -l)"

# 4. Run Schema linking and voting
python schema_linking.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --linked_json_pth ../../data/linked_${TASK}_tmp0.json \
    --reduce_col

# 5. (재확인) Prompt.txt 파일 중 200KB 이상인 파일 개수 확인
echo "Number of prompts.txt files in examples_${TASK} larger than 200KB after reducing: $(find examples_${TASK} -type f -name "prompts.txt" -exec du -b {} + | awk '$1 > 200000' | wc -l)"

#### Run ReFoRCE (Column Exploration 없이) ####

OUTPUT_PATH="output/${API}-${TASK}-no-exploration-log-${TIMESTAMP}"
NUM_VOTES=8
NUM_WORKERS=16
echo "AZURE mode: $AZURE"
echo "Model: $API"
echo "Task: $TASK"
echo "Output Path: $OUTPUT_PATH"
echo "🚫 Column Exploration: DISABLED"

# 단일 단계: Self-refinement + Majority Voting (Column Exploration 제외)
CMD="python run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_self_refinement \
    --generation_model ${API} \
    --max_iter 5 \
    --temperature 1 \
    --early_stop \
    --do_vote \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS"

if [ "$AZURE" = true ]; then
  CMD="$CMD --azure"
fi

eval $CMD
echo "Evaluation for Single Step (No Column Exploration)"
python eval.py --log_folder $OUTPUT_PATH --task $TASK

# Step 2: Random vote for tie
python run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_vote \
    --random_vote_for_tie \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS
echo "Evaluation for Step 2"
python eval.py --log_folder $OUTPUT_PATH --task $TASK

# Step 3: Random vote final_choose
python run.py \
    --task $TASK \
    --db_path examples_${TASK} \
    --output_path $OUTPUT_PATH \
    --do_vote \
    --random_vote_for_tie \
    --final_choose \
    --num_votes $NUM_VOTES \
    --num_workers $NUM_WORKERS
echo "Evaluation for Step 3"
python eval.py --log_folder $OUTPUT_PATH --task $TASK

# Final evaluation and get files for submission
python get_metadata.py --result_path $OUTPUT_PATH --output_path output/${API}-${TASK}-no-exploration-csv-${TIMESTAMP}
python get_metadata.py --result_path $OUTPUT_PATH --output_path output/${API}-${TASK}-no-exploration-sql-${TIMESTAMP} --file_type sql
cd ../../spider2-${TASK}/evaluation_suite
python evaluate.py --mode exec_result --result_dir ../../methods/ReFoRCE/output/${API}-${TASK}-no-exploration-csv-${TIMESTAMP} 