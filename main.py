#!/usr/bin/env python3
"""Main program for the active status update utility.

    Typical usage:
    ./main.py --stats_db $RECEIVER_DB_FILE \
              --member_db $CONTACTS_DB_FILE \
              --write_member_db

    - To dry-run the program without updating DB, remove --write_member_db
    - To save the update list in CSV, add --status_update_diff=$DIFF_CSV_FILE
    - To save the full list in CSV, add --status_update_full=$FULL_CSV_FILE
    - Logging is written to stdout by default.
"""
import argparse
import csv
import datetime
import sqlite3
import sys
import logging
from collections import namedtuple
from enum import Enum
from typing import Optional

from utils import db


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    stream=sys.stderr)
logger = logging.getLogger(__name__)


class ActivityStatus(Enum):
    STABLE = 'a'
    OCCASION = 's'
    GUEST = 'g'
    DEAD = 'd'
    NO_MORE = 'x'
    GUEST_VARIANCE_1 = 'n'
    GUEST_VARIANCE_2 = ''
    GUEST_VARIANCE_3 = 'v'


ActiveStatusUpdate = namedtuple(
    'ActiveStatusUpdate', ['all', 'diff'])


def get_attendance_encoded(stats_conn: sqlite3.Connection,
                           church_id: Optional[int] = None,
                           all_years: bool = False) -> list:
    """Gets encoded yearly attendance statistics for members.

    Gets rows from stats_db containing the yearly attendance for members.
    The encoding scheme is described in <returns>.

    Args:
      stats_conn: stats_db connection
      church_id: if provided, only returns the rows matching church_id
      all_years: if True, returns all rows regardless of year;
        else (default), only returns the rows for this year and the last year

    Returns:
      A list containing the fetched rows. Each row is a tuple
      (church_id, name, year, first_half, last_half).

      first_half is a 32-bit integer with bits (from MSB to LSB) representing
      the attendance for the 31st, 30th, 29th, ..., 0th week of year.

      last_half is a 32-bit integer with bits (from MSB to LSB) representing
      the attendance for the 53rd, 52nd, ..., 33rd week of year, and the bits
      preceding 53rd are all zeros.

      The week number follows Python's %W for the strptime() method,
      where Monday is considered the first day of week, and all days in a new
      year preceding the first Monday are considered to be in week 0.
      Therefore, the week number ranges from 0 to 53.
      Ref: https://docs.python.org/3/library/datetime.html
    """
    cnds_query, cnds_val = [], []
    if church_id:
        cnds_query.append('church_id=?')
        cnds_val.append(church_id)
    if not all_years:
        last_year = datetime.date.today().year - 1
        cnds_query.append('year>=?')
        cnds_val.append(last_year)

    cur = stats_conn.cursor()
    if cnds_query:
        res = cur.execute(
            'SELECT church_id, name, year, first_half, last_half'
            ' FROM receivers_record'
            f' WHERE {" AND ".join(cnds_query)}'
            ' ORDER BY church_id ASC, year ASC',
            tuple(cnds_val)
        )
    else:
        res = cur.execute(
            'SELECT church_id, name, year, first_half, last_half'
            ' FROM receivers_record'
            ' ORDER BY church_id ASC, year ASC',
        )
    return res.fetchall()


def _range_inclusive(start: int, end: int):
    return range(start, end+1)


def get_attendance(stats_conn: sqlite3.Connection,
                   church_id: Optional[int] = None,
                   end_date: Optional[datetime.date] = None) -> dict:
    """Gets the attendance statistics within a year.

    Gets the attendance statistics within a year dating from end_date.

    Args:
      stats_conn: stats_db connection
      church_id: if provided, only returns the records matching church_id
      end_date: the ending date for statistics; today by default

    Returns:
      A dict mapping a member's church_id to his attendance dict record

      {'church_id': church_id, 'name': name,
       'cnt': total_attendance_within_a_year,
       'bmp_end_year': encoded_attendance_bitmap_for_ending_year_in_len_54,
       'bmp_prv_year': encoded_attendance_bitmap_for_previous_year_in_len_54}
    """
    if end_date is None:
        end_date = datetime.date.today()
    this_year = datetime.date.today().year
    end_year = end_date.year
    end_week = int(end_date.strftime('%W'))
    attendance_yearly = get_attendance_encoded(
        stats_conn, church_id=church_id, all_years=(end_year==this_year)
    )

    attendance = {}
    for rec in attendance_yearly:
        church_id, name, year, first_half, last_half = rec
        try:
            att = attendance[church_id]
        except KeyError:
            att = {'church_id': church_id, 'name': name, 'cnt': 0,
                   'bmp_end_year': 0, 'bmp_prv_year': 0}
            attendance[church_id] = att
        if year == end_year:
            att['bmp_end_year'] = first_half | (last_half << 32)
        elif year == end_year-1:
            att['bmp_prv_year'] = first_half | (last_half << 32)

    bmp_mask = [1 << n for n in _range_inclusive(0, 53)]
    for _, att in attendance.items():
        bmp_end_year = att['bmp_end_year']
        bmp_prv_year = att['bmp_prv_year']
        att['cnt'] = \
            [bool(bmp_end_year & bmp_mask[bi])
             for bi in _range_inclusive(0, end_week)].count(True) + \
            [bool(bmp_prv_year & bmp_mask[bi])
             for bi in _range_inclusive(end_week+1, 53)].count(True)

    return attendance


def get_active_status(mem_conn: sqlite3.Connection,
                      church_id: Optional[int] = None) -> dict:
    """Gets active status from member_db.

    Args:
      mem_conn: member_db (aka. contacts_db) connection
      church_id: if provided, only returns the records matching church_id

    Returns:
      A dict mapping a member's church_id to his active status in tuple
      (church_id, name, presence)
    """
    cur = mem_conn.cursor()
    if church_id is None:
        res = cur.execute(
            'SELECT church_id, name, presence'
            ' FROM imports_person'
            ' ORDER BY church_id ASC'
        )
    else:
        res = cur.execute(
            'SELECT church_id, name, presence'
            ' FROM imports_person'
            ' WHERE church_id=?'
            ' ORDER BY church_id ASC',
            (church_id,)
        )

    rows = res.fetchall()
    return {r[0]: r for r in rows}


def get_new_active_status(attendance_cnt: int,
                          curr_active_status: ActivityStatus) \
        -> ActivityStatus:
    """Computes the active status"""
    if curr_active_status in (ActivityStatus.DEAD,
                              ActivityStatus.NO_MORE):
        return curr_active_status
    elif attendance_cnt >= 27:
        return ActivityStatus.STABLE
    elif attendance_cnt >= 1:
        return ActivityStatus.OCCASION
    else:
        return ActivityStatus.GUEST


def update_active_status(stats_conn: sqlite3.Connection,
                         mem_conn: sqlite3.Connection,
                         church_id: Optional[int] = None,
                         write_db: bool = False) \
        -> ActiveStatusUpdate:
    """Updates active status

    Args:
      stats_conn: stats_db connection
      mem_conn: member_db (aka. contacts_db) connection
      church_id: if provided, only updates the record matching church_id
      write_db: if True, write the updates to member_db;
        else (default), dry-run only

    Returns:
      An ActiveStatusUpdate named-tuple,
      ('all': a dict mapping all church_id to new_activity_status,
       'diff': a dict mapping affected church_id to
         (curr_activity_status, new_activity_status))
    """
    attendance = get_attendance(stats_conn, church_id=church_id)
    active_status = get_active_status(mem_conn, church_id=church_id)
    ret_all, ret_diff = {}, {}
    for cid, att in attendance.items():
        try:
            cur_act = ActivityStatus(active_status[cid][2])
            new_act = get_new_active_status(att['cnt'], cur_act)
            ret_all[cid] = new_act
            if new_act != cur_act:
                ret_diff[cid] = (cur_act, new_act)
        except KeyError:
            logger.warning('church_id %d found in stats_db'
                           ' but unfound in member_db',
                           cid)

    if write_db:
        if ret_diff:
            cur = mem_conn.cursor()
            # print([(act[1].value, cid) for cid, act in ret_diff.items()])
            cur.executemany(
                'UPDATE imports_person SET presence=? WHERE church_id=?',
                ((act[1].value, cid) for cid, act in ret_diff.items())
            )
            mem_conn.commit()
            logger.info('%d records updated in member_db', cur.rowcount)
        else:
            logger.info('No records need updates in member_db')

    return ActiveStatusUpdate(ret_all, ret_diff)


def write_active_status_diff(write_file_path: str,
                             attendance,
                             status_update: ActiveStatusUpdate):
    """Writes diff activity status changes.

    The change file is in CSV format containing
    church_id, name, cnt, curr_status, new_status.

    Args:
      write_file_path: CSV file to write
      attendance: return value from get_attendance()
      status_update: return value from update_active_status()
    """
    with open(write_file_path, 'wt', encoding='utf-8', newline='') as fout:
        csv_writer = csv.writer(fout)
        csv_writer.writerow(['church_id', 'name', 'cnt',
                             'curr_status', 'new_status'])
        for cid, att_diff in status_update.diff.items():
            try:
                name = attendance[cid]['name']
                cnt = attendance[cid]['cnt']
                curr_status = att_diff[0].name
                new_status = att_diff[1].name

                csv_writer.writerow([cid, name, cnt,
                                     curr_status, new_status])
            except KeyError:
                pass


def write_active_status_full(write_file_path: str,
                             attendance,
                             status_update: ActiveStatusUpdate):
    """Writes full activity status changes.

    The change file is in CSV format containing
    church_id, name, cnt, curr_status, new_status, diff.

    Args:
      write_file_path: CSV file to write
      attendance: return value from get_attendance()
      status_update: return value from update_active_status()
    """
    with open(write_file_path, 'wt', encoding='utf-8', newline='') as fout:
        csv_writer = csv.writer(fout)
        csv_writer.writerow(['church_id', 'name', 'cnt',
                             'curr_status', 'new_status', 'diff'])
        for cid, att in attendance.items():
            diff = cid in status_update.diff
            try:
                if diff:
                    curr_status = status_update.diff[cid][0].name
                else:
                    curr_status = status_update.all[cid].name
                new_status = status_update.all[cid].name

                csv_writer.writerow([cid, att['name'], att['cnt'],
                                     curr_status, new_status, diff])
            except KeyError:
                pass


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stats_db', type=str,
                        help='stats db file path')
    parser.add_argument('--member_db', type=str,
                        help='contacts (member) db file path')
    parser.add_argument('--write_member_db', action='store_true', default=False,
                        help='write active status update to member db')
    parser.add_argument('--status_update_diff', type=str,
                        help='diff active status update CSV file path')
    parser.add_argument('--status_update_full', type=str,
                        help='full active status update CSV file path')
    return parser.parse_args()


def _main():
    args = _get_args()

    db_mgr = db.DBManager(stats_db=args.stats_db, member_db=args.member_db)

    attendance = get_attendance(db_mgr.stats_db)
    status_update = update_active_status(db_mgr.stats_db,
                                         db_mgr.mem_db,
                                         write_db=args.write_member_db)
    if args.status_update_diff:
        write_active_status_diff(args.status_update_diff,
                                 attendance,
                                 status_update)
    if args.status_update_full:
        write_active_status_full(args.status_update_full,
                                 attendance,
                                 status_update)

    db_mgr.stats_db.close()
    db_mgr.mem_db.close()


if __name__ == '__main__':
    _main()
