#!/usr/bin/env python3

import argparse
import os
import re
import json
import zlib
from collections import defaultdict


def find_logs(path):
    p_pattern = re.compile(r'^p.log$')
    c_pattern = re.compile(r'^c.log$')
    consumers = []
    producers = []
    for f in os.listdir(path):
        if p_pattern.match(f):
            producers.append(os.path.abspath(os.path.join(path, f)))
        elif c_pattern.match(f):
            consumers.append(os.path.abspath(os.path.join(path, f)))
    return producers, consumers


class Record:
    def __init__(self, offset, value):
        self.offset = offset
        self.value = value

    def __str__(self):
        return "v:{}, o:{}".format(self.value, self.offset)

    def __repr__(self):
        return "v:{}, o:{}".format(self.value, self.offset)


def by_offset(r):
    return r.offset


def parse_entry(entry):
    if not entry.startswith("ack"):
        return None
    parts = entry.split(",")
    if len(parts) != 4:
        return None
    return (int(parts[1]), int(parts[2]), int(parts[3]))


def main():
    parser = argparse.ArgumentParser(description='Log analyzer')
    parser.add_argument('input', type=str, help='log filefile')

    args = parser.parse_args()
    producer_logs, consumer_logs = find_logs(args.input)
    consumed = defaultdict(list)
    for l in consumer_logs:
        print('parsing: {}'.format(l))
        with open(l, 'r') as f:
            for entry in f:
                r = parse_entry(entry)
                if not r:
                    continue
                consumed[r[0]].append(Record(r[1], r[2]))

    produced = defaultdict(list)

    for l in producer_logs:
        print('parsing: {}'.format(l))
        with open(l, 'r') as f:
            for entry in f:
                r = parse_entry(entry)
                if not r:
                    continue
                produced[r[0]].append(Record(r[1], r[2]))

    total = 0
    for p in produced.items():
        part = p[0]
        produced_records = p[1]
        consumed_records = consumed[part]
        produced_records.sort(key=by_offset)
        consumed_records.sort(key=by_offset)
        total += len(produced_records)
        print('produced {} records at partition {}'.format(
            len(produced_records), p[0]))
        if len(produced_records) != len(consumed_records):
            print(
                "partition {} record count missmatch, produced: {}, consumed: {}"
                .format(p[0], len(produced_records), len(consumed_records)))

        c_idx = 0
        for pr in produced_records:
            while consumed_records[c_idx].offset < pr.offset:
                c_idx += 1

            if consumed_records[c_idx].offset == pr.offset:
                if pr.value != consumed_records[c_idx].value:
                    raise Exception(
                        "record value missmatch at offset: {pr.offset}".format(
                            pr.offset))
                c_idx += 1
            else:
                raise Exception(
                    "expected offset: {}, consumed offset: {}".format(
                        pr.offset, consumed_records[c_idx].offset))

    print('toal {} records produced'.format(total))


if __name__ == "__main__":
    main()
