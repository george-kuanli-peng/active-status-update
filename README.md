# active-status-update

Active status update utilities.

## Requirements

- Python 3.8+

## Usage

### Main script

main.py:

    Typical usage:
    ./main.py --stats_db $RECEIVER_DB_FILE \
              --member_db $CONTACTS_DB_FILE \
              --write_member_db

    - To dry-run the program without updating DB, remove --write_member_db
    - To save the update list in CSV, add --status_update_diff=$DIFF_CSV_FILE
    - To save the full list in CSV, add --status_update_full=$FULL_CSV_FILE
    - Logging is written to stdout by default.

### Run helper

run.sh:

Preparation:

1. Set the variables in the beginning of script.
1. Create the $DB_BKUP_DIR and $RESULT_DIR directories if they have not existed.

Typical usage:

```bash
./run.sh
```
