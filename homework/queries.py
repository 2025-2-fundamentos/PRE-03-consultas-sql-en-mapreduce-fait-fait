"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from mapreduce import mapreduce  # type: ignore
import shutil
import os
#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#
# QUERY 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#

def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append(
                (index, row.strip() + ",tip_rate")
            )
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence







