import csv
import io
import os
import socketserver
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler
from lm.chrome import footer, lookup_form, parcel_link, shell, topline
from lm.config import EXPORT_CAP, PORT
from lm.filters import Filt, count_parcels, order_by
from lm.pages_errors import page_404
from lm.pages_export import EXPORT_OWNER_COLS, EXPORT_PARCEL_COLS, export_owner_rows, export_parcel_rows
from lm.pages_health import page_health
from lm.pages_home import page_home, page_search
from lm.pages_method import page_method
from lm.pages_owner import page_owner
from lm.pages_parcel import page_parcel, page_pid_choice
from lm.pages_rankings import page_explore, page_rankings
from lm.ranksql import rank_owners_count
from lm.skin import BRAND_DIR, BRAND_FILES, DEFAULT_SKIN, SKINS, SKIN_COOKIE, _CURRENT, set_skin
from lm.sql import DB_PATH
from lm.store import STORE

# ---------------------------------------------------------------------------
# http
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "landlord-mapper-ui/1.0"
    protocol_version = "HTTP/1.1"

    # Set per request by pick_skin() when ?skin= asked for a change. A class
    # default because a 500 can reach send_html() without routing.
    skin_cookie = None

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.log_date_time_string(), fmt % args))

    def send_skin_cookie(self):
        if self.skin_cookie:
            self.send_header("Set-Cookie", self.skin_cookie)

    def send_html(self, html_text, code=200):
        payload = html_text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_skin_cookie()
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def redirect(self, to):
        self.send_response(303)
        self.send_header("Location", to)
        self.send_header("Content-Length", "0")
        self.send_skin_cookie()
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            self.route()
        except BrokenPipeError:
            pass
        except Exception:
            import traceback
            traceback.print_exc()
            try:
                self.send_html(page_error(), 500)
            except Exception:
                pass

    def stream_csv(self, f):
        """Streamed, never buffered: a filtered export can be a quarter of a
        million rows and holding that in memory is how this process died once.
        HTTP/1.1 with no Content-Length needs the connection closed at the end,
        which is what Connection: close declares."""
        if f.owner:
            o = STORE.owners.get(f.owner)
            if o is None:
                return self.send_html(page_404("owner", f.owner), 404)
            total = o["n_parcels"]
            cols = EXPORT_PARCEL_COLS
            rows = export_parcel_rows("p.owner_id = ?", (f.owner,),
                                      order_by(f, "p."), EXPORT_CAP)
            name = "landlord-mapper_owner-%s_by-%s" % (f.owner, f.sort)
            what = "parcels held by this owner"
        elif f.shape == "owners":
            cols = EXPORT_OWNER_COLS
            total = rank_owners_count(f)[0]
            rows = export_owner_rows(f)
            name = "landlord-mapper_%s" % f.slug()
            what = "owners ranked over in-scope parcels"
        else:
            w, a = f.where("p.")
            total = count_parcels(f)
            # the CSV is the population the table showed, in the order the
            # table showed it, with no cap on either side any more
            cols = EXPORT_PARCEL_COLS
            rows = export_parcel_rows(w, a, order_by(f, "p."), EXPORT_CAP)
            name = "landlord-mapper_%s" % f.slug()
            what = "parcels matching the filter"
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition",
                         "attachment; filename=\"%s.csv\"" % name)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command == "HEAD":
            return
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\r\n")
        w.writerow(cols)
        n = 0
        try:
            for row in rows:
                w.writerow(row)
                n += 1
                if n % 2000 == 0:
                    self.wfile.write(buf.getvalue().encode("utf-8"))
                    buf.seek(0)
                    buf.truncate(0)
            if total > n:
                # never truncate silently
                w.writerow(["# TRUNCATED: %d of %d %s written, at the %d row export "
                            "cap. Narrow the filter, by county or ZIP, to get the rest"
                            % (n, total, what, EXPORT_CAP)])
            self.wfile.write(buf.getvalue().encode("utf-8"))
            self.wfile.flush()
        except BrokenPipeError:
            pass

    def cookie_skin(self):
        """The skin the browser last chose. Parsed by hand rather than with
        http.cookies because one name is wanted out of a header that a proxy or
        an analytics script may have filled with anything, and SimpleCookie
        silently drops the WHOLE header when any single morsel is malformed."""
        raw = self.headers.get("Cookie") or ""
        for part in raw.split(";"):
            k, _, v = part.strip().partition("=")
            if k == SKIN_COOKIE and v in SKINS:
                return v
        return None

    def pick_skin(self, qs):
        """?skin= wins over the cookie, so a link can carry the skin to someone
        who has never chosen one. Returns the Set-Cookie value when the choice
        needs persisting, so a switch survives the next click."""
        want = (qs.get("skin", [""])[0] or "").strip().lower()
        if want in SKINS:
            set_skin(want)
            # Lax, not None: this is a display preference, it never needs to
            # travel on a cross-site request, and it holds nothing about anyone.
            return ("%s=%s; Path=/; Max-Age=31536000; SameSite=Lax"
                    % (SKIN_COOKIE, want))
        set_skin(self.cookie_skin() or DEFAULT_SKIN)
        return None

    def send_brand(self, name):
        """The chapter's logo files and the two brand faces. Immutable because
        the filenames are the branding kit's own and their contents do not change
        without a new name; without it every page paints Styrene twice."""
        ctype = BRAND_FILES.get(name)
        if ctype is None:
            return self.send_html(page_404("file", name), 404)
        try:
            with open(os.path.join(BRAND_DIR, name), "rb") as fh:
                blob = fh.read()
        except OSError:
            # The skin is still usable without its fonts -- they degrade to the
            # fallback grotesque -- so this is a 404, not a 500.
            return self.send_html(page_404("file", name), 404)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(blob)))
        self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(blob)

    def route(self):
        u = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(u.path)
        qs = urllib.parse.parse_qs(u.query)

        # Before any page is built: shell(), topline() and skinswitch() read the
        # skin and the url off the thread, not off an argument.
        _CURRENT.url = self.path
        self.skin_cookie = self.pick_skin(qs)

        if path.startswith("/brand/"):
            return self.send_brand(path[len("/brand/"):].strip("/"))
        if path in ("/", "/index.html"):
            return self.send_html(page_home())
        if path in ("/health", "/healthz", "/health.html"):
            return self.send_html(page_health())
        if path == "/favicon.ico":
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path == "/search":
            q = (qs.get("q", [""])[0] or "").strip()
            if not q:
                return self.redirect("/")
            try:
                page = int(qs.get("page", ["1"])[0])
            except ValueError:
                page = 1
            html_text, single = page_search(q, page)
            if single is not None:
                return self.redirect(parcel_link(single))
            return self.send_html(html_text)
        if path.startswith("/parcel/"):
            # /parcel/<county>/<pid> is the canonical form. /parcel/<pid> is
            # still honoured, because old links carry it, and it resolves when
            # exactly one roll carries that ID; when several do it asks which.
            rest = path[len("/parcel/"):].strip("/")
            if "/" in rest:
                county, pid = rest.split("/", 1)
            else:
                county, pid = "", rest
            cands = STORE.pid_candidates(pid, county or None)
            if not cands:
                return self.send_html(page_404("parcel", rest), 404)
            if len(cands) > 1:
                return self.send_html(page_pid_choice(pid, cands))
            return self.send_html(page_parcel(cands[0]))
        if path.startswith("/owner/"):
            oid = path[len("/owner/"):].strip("/")
            out = page_owner(oid, qs)
            if out is None:
                return self.send_html(page_404("owner", oid), 404)
            return self.send_html(out)
        if path == "/rankings":
            return self.send_html(page_rankings(Filt.from_qs(qs)))
        if path == "/explore":
            return self.send_html(page_explore(Filt.from_qs(qs)))
        if path == "/method":
            return self.send_html(page_method())
        if path in ("/export.csv", "/export"):
            return self.stream_csv(Filt.from_qs(qs))
        return self.send_html(page_404("page", path), 404)

def page_error():
    body = [
        "<header class=\"wrap masthead\">", topline(),
        "<h1 style=\"font-size:clamp(1.6rem,6vw,2.9rem)\">Something<br /><em>broke</em></h1>",
        "</header>",
        "<section class=\"wrap band\" id=\"main\" style=\"padding-top:0\">",
        "<div class=\"empty\"><h3>This page failed to render</h3>"
        "<p>The failure is written to the server log with a traceback. The data in memory is "
        "untouched, so the lookup above still works</p></div>",
        "<div style=\"margin-top:1.4rem\">", lookup_form(), "</div>",
        "</section>", footer()]
    return shell("Error - Landlord Mapper", "".join(body))

class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

def main():
    t0 = time.time()
    if not os.path.exists(DB_PATH):
        sys.stderr.write("FATAL: no database at %s\n" % DB_PATH)
        sys.stderr.write("Build it from the CSVs first: python3 build-db.py\n")
        raise SystemExit(2)
    sys.stderr.write("opening %s (%.0f MB)\n"
                     % (DB_PATH, os.path.getsize(DB_PATH) / 1048576.0))
    STORE.load()
    st = STORE.stats
    sys.stderr.write(
        "parcel file %s (written %s)\n"
        % (st["parcel_file"], st["parcel_mtime"]))
    sys.stderr.write(
        "loaded %s parcels, %s owners, %s scrape rows in %ss\n"
        % (st["parcel_rows"], st["owners"], st["scrape_rows"], st["load_seconds"]))
    sys.stderr.write(
        "county rolls: %r\n" % (st["counties"],))
    sys.stderr.write(
        "%s distinct parcel IDs, %s of them carried by more than one roll\n"
        % (st["parcel_pids"], st["parcel_pids_shared"]))
    sys.stderr.write(
        "in scope: %s parcels, %s owners; %s in-scope owners answered\n"
        % (st["parcels_in_scope"], st["owners_in_scope"],
           st["owners_in_scope_answered"]))
    sys.stderr.write(
        "scrape rows joined %s, held back: no parcel %s, address clash %s\n"
        % (st["scrape_rows_joined"], st["scrape_rows_no_parcel"],
           st["scrape_rows_addr_clash"]))
    sys.stderr.write("scrape_status rows: %r\n" % (st["scrape_status_rows"],))
    sys.stderr.write("owner states: %r\n" % (st["owner_states"],))
    for w in st.get("errors", []):
        sys.stderr.write("WARNING: %s\n" % w)
    sys.stderr.write(
        "database opened in %.2fs; those figures were computed by build-db.py "
        "and are read, not recomputed\n" % (time.time() - t0))
    srv = Server(("0.0.0.0", PORT), Handler)
    sys.stderr.write("serving on 0.0.0.0:%d\n" % PORT)
    sys.stderr.flush()
    srv.serve_forever()

if __name__ == "__main__":
    main()
