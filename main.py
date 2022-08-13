#!/usr/bin/env python3
"""Main program for the active status update utility.

    Typical usage:
    ./main.py
"""
import csv
import datetime
import sqlite3
import logging
from collections import namedtuple
from enum import Enum
from typing import Optional


logger = logging.getLogger()
logger.setLevel(logging.INFO)
_stats_conn = None
_mem_conn = None


class ActivityStatus(Enum):
    STABLE = 'a'
    OCCASION = 's'
    GUEST = 'g'
    DEAD = 'd'
    NO_MORE = 'x'
    UNKNOWN_1 = 'n'  # TODO
    UNKNOWN_2 = ''   # TODO
    UNKNOWN_3 = 'v'  # TODO


ActiveStatusUpdate = namedtuple(
    'ActiveStatusUpdate', ['all', 'diff'])


def get_stats_conn() -> sqlite3.Connection:
    global _stats_conn

    if _stats_conn is None:
        _stats_conn = sqlite3.connect('../test_db/20220808_receiverdb.sqlite3')

    return _stats_conn


def get_member_conn() -> sqlite3.Connection:
    global _mem_conn

    if _mem_conn is None:
        _mem_conn = sqlite3.connect('../test_db/20220809_ContactsDB')

    return _mem_conn


def get_attendance_encoded(stats_conn: sqlite3.Connection,
                           church_id: Optional[int] = None,
                           all_years: bool = False):
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
                   end_date: Optional[datetime.date] = None):
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
    for church_id, att in attendance.items():
        bmp_end_year = att['bmp_end_year']
        bmp_prv_year = att['bmp_prv_year']
        att['cnt'] = \
            [bool(bmp_end_year & bmp_mask[bi])
             for bi in _range_inclusive(0, end_week)].count(True) + \
            [bool(bmp_prv_year & bmp_mask[bi])
             for bi in _range_inclusive(end_week+1, 53)].count(True)

    return attendance


def get_active_status(mem_conn: sqlite3.Connection,
                      church_id: Optional[int] = None):
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
    if curr_active_status in (ActivityStatus.DEAD,
                              ActivityStatus.NO_MORE):
        return curr_active_status
    elif attendance_cnt >= 27:
        return ActivityStatus.STABLE
    elif attendance_cnt >= 1:
        return ActivityStatus.OCCASION
    else:
        # TODO: what if curr_active_status != GUEST
        return ActivityStatus.GUEST


def update_active_status(stats_conn: sqlite3.Connection,
                         mem_conn: sqlite3.Connection,
                         church_id: Optional[int] = None,
                         write_db: bool = False) \
        -> ActiveStatusUpdate:
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

    return ActiveStatusUpdate(ret_all, ret_diff)


def _main():
    stats_conn = get_stats_conn()
    mem_conn = get_member_conn()

    attendance = get_attendance(stats_conn)
    status_update = update_active_status(stats_conn, mem_conn)
    with open('result.csv', 'wt', encoding='utf-8', newline='') as fout:
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

    stats_conn.close()
    mem_conn.close()


if __name__ == '__main__':
    _main()
