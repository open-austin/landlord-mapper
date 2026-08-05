#!/usr/bin/env python3
"""Entry point. The server itself lives in the lm package.

server.py was a single 4,600-line module. It is now a package so that a reader
looking for the CSS, the SQL, or one page's markup opens one file instead of
scrolling past the other three. Nothing about the served bytes changed: the
split was verified by replaying 143 routes against both versions and comparing
sha256 per response body.

Start here, then lm/app.py for the routing table.
"""
from lm.app import main

if __name__ == "__main__":
    main()
