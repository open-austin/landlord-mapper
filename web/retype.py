#!/usr/bin/env python3
"""Convert an existing untyped landlord-mapper database into the typed one.

    retype.py <src.sqlite3> <dst.sqlite3>

The source is opened read-only and never written: the file is copied first and
the typing pass runs on the copy, so the live database the UI is serving from is
untouched.  build-db.py calls the same typed.retype() at the end of a fresh
build, so a refresh-data.sh run and this script produce the same schema.

Steps: copy -> retype (typed.py) -> ANALYZE -> VACUUM.  The VACUUM is what
actually reclaims the pages the dropped columns freed, and it transiently needs
room for a second full copy.
"""
import os
import shutil
import sqlite3
import sys
import time

import typed


def log(msg):
    sys.stderr.write("[retype] %s\n" % msg)
    sys.stderr.flush()


def mb(path):
    return os.path.getsize(path) / 1048576.0


def main():
    src, dst = sys.argv[1], sys.argv[2]
    if os.path.abspath(src) == os.path.abspath(dst):
        raise SystemExit("retype: src and dst must differ")
    t0 = time.time()
    work = dst + ".building"
    for junk in (work, work + "-journal", work + "-wal", work + "-shm"):
        if os.path.exists(junk):
            os.unlink(junk)
    log("copying %s (%.1f MB) -> %s" % (src, mb(src), work))
    shutil.copyfile(src, work)

    cx = sqlite3.connect(work)
    cx.executescript("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")
    cx.execute("PRAGMA cache_size = -2000000")      # 2 GB of page cache
    typed.retype(cx, log=log)
    log("analyze")
    cx.execute("ANALYZE")
    cx.commit()
    log("before vacuum: %.1f MB" % mb(work))
    cx.execute("VACUUM")
    cx.commit()
    cx.close()
    log("after vacuum: %.1f MB" % mb(work))
    os.replace(work, dst)
    os.chmod(dst, 0o644)
    log("wrote %s, %.1f MB (%d bytes) in %.1fs"
        % (dst, mb(dst), os.path.getsize(dst), time.time() - t0))


main()
