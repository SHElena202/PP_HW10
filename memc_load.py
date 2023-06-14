#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from asyncio.subprocess import Process
from itertools import islice
from multiprocessing import JoinableQueue
from optparse import OptionParser

from anyio import Lock, key
from faker.providers import address
from numpy.array_api._array_object import Array

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache



NORMAL_ERR_RATE = 0.01
PACKEGE_SIZE = 500
AppsInstalled = collections.namedtuple(
    "AppsInstalled",
    ["dev_type", "dev_id", "lat", "lon", "apps"],
)

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(app_type, memc_client, apps, dry_run=False):
    package, log_package = {}, []
    for appsinstalled in apps:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)

        if dry_run:
            log_package.append((key, ua))
        else:
            packed = ua.SerializeToString()
            package.update({key:packed})

        if dry_run:
            for key, ua in log_package:
                logging.debug("%s - %s -> %s" % (app_type, key, str(ua).replace("\n", " ")))
        else:
            try:
                return len(memc_client.set_multi(package))
            except Exception as e:
                logging.exception("Cannot write to memc %s: %s" % (app_type, e))
                return len(apps)


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_file(io_queue, file_stats, device_memc, memc_clients, options, lock):
    logging.basicConfig(filename=options.log,
                        level=logging.INFO if not options.dry else logging.DEBUG,
                        format='[%(astime)s] %(levelname).1s %(msg)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    while True:
        package = io_queue.get()
        errors, processed = 0, 0
        apps = dict((app_type, []) for app_type in device_memc)

        for line in package:
            appsinstalled = parse_appsinstalled(line)

            if not appsinstalled:
                errors += 1
                continue

            if apps.get(appsinstalled.dev_type) is None:
                errors += 1
                continue

            apps[appsinstalled.dev_type].append(appsinstalled)

        for app_type, _apps in apps.items():
            if not _apps:
                continue
            _errors = insert_appsinstalled(app_type, memc_clients[app_type], _apps, options.dry)
            processed += len(_apps) - _errors
            errors += _errors

        with lock:
            file_stats[0] += errors
            file_stats[1] += processed

        io_queue.task_done()

def produce(io_queue, options, workers, file_stats):
    for fn in sorted(glob.inglob(options.pattern)):
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn, 'rt')

        package = list(islice(fd, PACKEGE_SIZE))
        while package:
            io_queue.put(package)
            package = list(islice(fd, PACKEGE_SIZE))

        io_queue.join()

        if not file_stats[1]:
            file_stats[0], file_stats[1] = 0, 0
            fd.close()
            dot_rename(fn)
            continue

        err_rate = file_stats[0] / file_stats[1]
        file_stats[0], file_stats[1] = 0, 0

        if err_rate < NORMAL_ERR_RATE:
            logging.info('Acceptable error rate (%s). Successfull load' % err_rate)
        else:
            logging.error('High error rate (%s > %s). Failed load' % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename()

        for worker in workers:
            worker.terminate()



def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    lock = Lock()
    file_stats = Array(typecode_or_type='i', size_or_initializer=2)

    memc_clients = dict((key, memcache.Client([address]))
                        for key, address in device_memc.items())

    io_queue = JoinableQueue()

    workers = []
    for i in range(options.workers):
        p = Process(target=process_file, args=(io_queue, file_stats, device_memc, memc_clients, options,lock))
        p.start()
        workers.append(p)

    produce(io_queue, options, workers,file_stats)


'''
    for fn in glob.iglob(options.pattern):
        processed = errors = 0
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        for line in fd:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue
            ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
            if ok:
                processed += 1
            else:
                errors += 1
        if not processed:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(errors) / processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)
'''


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
