#!/bin/bash
set -e

# NOTE: set the following variables,
#       and create the $DB_BKUP_DIR and $RESULT_DIR directories if they have not existed.
STATS_DB_FILE=/path/to/stats/db/file
MEMBER_DB_FILE=/path/to/member/db/file
DB_BKUP_DIR=/path/to/db/bakcup/dir
DB_BKUP_PREFIX=ContactsDB
DB_BKUP_CNT=8
RESULT_DIR=/path/to/result/dir


ensure_dir_exists() {
	local test_dir="$1"
	if [ ! -d "$test_dir" ]; then
		echo "[ERROR] Directory $test_dir does not exist"
		exit 1
	fi
}


gen_db_bkup_name() {
	local date_suffix=$(date +"%Y%m%d-%H%M")
	echo "${DB_BKUP_PREFIX}_${date_suffix}"
}


gen_result_name() {
	local date_suffix=$(date +"%Y%m%d-%H%M")
	echo "result_diff_${date_suffix}.csv"
}


ensure_file_nonexists() {
	local test_file="$1"
	if [ -f "$test_file" ]; then
		echo "[ERROR] Backup file $test_file already exists"
		exit 1
	fi
}


ensure_db_bkup_cnt() {
	local db_bkup_files=($(find "$DB_BKUP_DIR" -name "$DB_BKUP_PREFIX*" | sort -r))
	local db_bkup_cnt=${#db_bkup_files[@]}
	for (( i=$DB_BKUP_CNT; i < "$db_bkup_cnt"; i++ )); do
		echo "Delete ${db_bkup_files[$i]}"
		rm -f "${db_bkup_files[$i]}"
	done
}

ensure_dir_exists "$DB_BKUP_DIR"
ensure_dir_exists "$RESULT_DIR"
db_bkup_file="$DB_BKUP_DIR/$(gen_db_bkup_name)"
ensure_file_nonexists "$db_bkup_file"
result_file="$RESULT_DIR/$(gen_result_name)"

cp "$MEMBER_DB_FILE" "$db_bkup_file"

./main.py \
	--stats_db "$STATS_DB_FILE" \
	--member_db "$MEMBER_DB_FILE" \
	--status_update_diff "$result_file" \
	--write_member_db

ensure_db_bkup_cnt
