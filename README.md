# active-status-update

Active status update utilities.

## Requirements

- Python 3.8+

## Usage

    Typical usage:
    ./main.py --stats_db $RECEIVER_DB_FILE \
              --member_db $CONTACTS_DB_FILE \
              --write_member_db

    To dry-run the program without updating DB, remove --write_member_db
    To save the update list in CSV, add --status_update_diff=DIFF_CSV_FILE
    Logging is written to stdout by default.