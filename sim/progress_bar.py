#!/usr/bin/env python
"""Prints a progress bar."""

def print_progress(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '*',
                   printEnd = "\r"):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r|{bar}| {percent}%', end=printEnd)
    if iteration >= total:
        print()