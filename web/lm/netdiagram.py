from lm.coerce import norm_txt
from lm.fmt import e, money, num, title_case
from lm.schema import MATCHED, NOT_LOOKED_UP, NOT_RESOLVED, NO_RECORD, OUT_OF_SCOPE
from lm.store import STORE

# ---------------------------------------------------------------------------
# the shell network
# ---------------------------------------------------------------------------
EDGE_CLASS = {"officer": "e-line--officer", "agent": "e-line--agent",
              "mail": "e-line--mail"}

EDGE_TEXT = {"officer": "SHARED OFFICER", "agent": "SHARED REGISTERED AGENT",
             "mail": "SHARED MAILING ADDRESS"}

def wrap_name(s, width=24, lines=2):
    words = (s or "").split()
    out, cur = [], ""
    for w in words:
        cand = (cur + " " + w).strip()
        if len(cand) > width and cur:
            out.append(cur)
            cur = w
        else:
            cur = cand
        if len(out) == lines:
            break
    if cur and len(out) < lines:
        out.append(cur)
    if not out:
        out = ["(unnamed)"]
    if len(out) == lines and len(" ".join(words)) > sum(len(x) for x in out) + 1:
        out[-1] = out[-1][:width - 1] + "…"
    return out

def net_box(x, y, w, oid, label, state, sub, focus=False):
    lines = wrap_name(label)
    sw = {MATCHED: "sw-fill", NO_RECORD: "sw-ink"}.get(state, "sw-hollow")
    parts = ["<a class=\"n-link\" href=\"/owner/%s\">" % e(oid) if not focus else ""]
    parts.append("<rect class=\"n-box%s\" x=\"%d\" y=\"%d\" width=\"%d\" height=\"80\"%s />"
                 % (" n-box--focus" if focus else "", x, y, w,
                    " stroke-dasharray=\"7 4\""
                    if state in (NOT_LOOKED_UP, NOT_RESOLVED, OUT_OF_SCOPE)
                    and not focus else ""))
    ty = y + 26
    for ln in lines:
        parts.append("<text class=\"n-name\" x=\"%d\" y=\"%d\">%s</text>"
                     % (x + 15, ty, e(ln)))
        ty += 18
    parts.append("<rect class=\"%s\" x=\"%d\" y=\"%d\" width=\"9\" height=\"9\" />"
                 % (sw, x + 15, y + 60))
    parts.append("<text class=\"n-state\" x=\"%d\" y=\"%d\">%s</text>"
                 % (x + 30, y + 68, e(sub)))
    if not focus:
        parts.append("</a>")
    return "".join(parts)

def edge(x1, y1, x2, y2, kind, text):
    cls = EDGE_CLASS[kind]
    mid_x = (x1 + x2) / 2.0
    mid_y = (y1 + y2) / 2.0
    w = len(text) * 6.3 + 14
    weak = " e-label--weak" if kind == "agent" else ""
    return (
        "<line class=\"e-line %s\" x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" />"
        "<rect class=\"e-knock\" x=\"%.0f\" y=\"%.0f\" width=\"%.0f\" height=\"18\" />"
        "<text class=\"e-label%s\" x=\"%.0f\" y=\"%.0f\">%s</text>"
        % (cls, x1, y1, x2, y2, mid_x - w / 2, mid_y - 9, w, weak,
           mid_x - w / 2 + 7, mid_y + 4, e(text)))

def owner_sub(oid):
    o = STORE.owners[oid]
    tot = STORE.owner_totals(o)
    state = o.get("state", NOT_LOOKED_UP)
    if state == MATCHED:
        return "MATCHED · %s · %s PROPERTIES" % (money(tot["value"]), num(tot["count"]))
    if state == NO_RECORD:
        return "NO RECORD · %s PROPERTIES" % num(tot["count"])
    if state == OUT_OF_SCOPE:
        return "OUTSIDE COVERAGE · %s PROPERTIES" % num(tot["count"])
    return "NOT LOOKED UP · %s PROPERTIES" % num(tot["count"])

def network_panel(oid, state, fl):
    nb = STORE.neighbourhood(oid)
    if nb is None:
        return (
            "<div class=\"netwrap\"><div class=\"netnote\" style=\"padding-top:clamp(0.9rem,3vw,1.3rem)\">"
            "<p>There is no network to draw yet. Linking one company to another needs a "
            "franchise filing on both ends, and this owner has no matched filing. The links "
            "this tool will draw are a shared officer, a shared registered agent, and a "
            "shared mailing address, in that order of strength</p></div></div>")

    hop1, hop2 = nb["hop1"], nb["hop2"]
    if not hop1 and not hop2:
        note = ["<p>Nothing else in this data shares an officer, a registered agent, or a "
                "mailing address with this filing. That is a real answer about this owner, "
                "not a blank</p>"]
        for kind, key, n in nb["hubs"]:
            note.append(
                "<p>One link was withheld. The %s on this filing, %s, appears on %s other "
                "filings here. At that scale it is a hub, not a relationship, so it is "
                "reported as a count instead of drawn as lines</p>"
                % (e(EDGE_TEXT[kind].lower().replace("shared ", "")), e(title_case(key)),
                   num(n)))
        return ("<div class=\"netwrap\"><div class=\"netnote\" "
                "style=\"padding-top:clamp(0.9rem,3vw,1.3rem)\">%s</div>%s</div>"
                % ("".join(note), edge_key()))

    COL0, W0 = 8, 250
    COL1, W1 = 470, 250
    COL2, W2 = 980, 250
    ROW = 108
    rows = max(len(hop1), len(hop2), 1)
    height = 24 + rows * ROW
    width = 1240 if hop2 else 730

    y_of_1 = {}
    svg = []
    focus_y = 24 + (rows * ROW - 80) / 2.0

    # edges first so boxes sit on top
    for n, (pid1, reasons) in enumerate(hop1):
        y1 = 24 + n * ROW
        y_of_1[pid1] = y1
        kind = min(reasons, key=lambda r: {"officer": 0, "mail": 1, "agent": 2}[r[0]])[0]
        text = EDGE_TEXT[kind]
        if kind == "agent":
            fan = STORE.agent_fanout(norm_txt(fl.get("agent"))) - 1
            text = "%s · %s MORE HERE" % (text, num(max(fan, 0)))
        svg.append(edge(COL0 + W0, focus_y + 40, COL1, y1 + 40, kind, text))
        extra = [r for r in reasons if r[0] != kind]
        if extra:
            labels = ", ".join(sorted(set(EDGE_TEXT[k].lower() for k, _ in extra)))
            svg.append("<text class=\"n-state\" x=\"%d\" y=\"%d\">ALSO %s</text>"
                       % (COL1 + 15, y1 - 6, e(labels.upper())))
    for n, (pid2, parent, kind, _t) in enumerate(hop2):
        y2 = 24 + n * ROW
        py = y_of_1.get(parent, focus_y)
        svg.append(edge(COL1 + W1, py + 40, COL2, y2 + 40, kind, EDGE_TEXT[kind]))

    o = STORE.owners[oid]
    svg.append(net_box(COL0, focus_y, W0, oid,
                       fl.get("corp_name") or o["name"], state, owner_sub(oid),
                       focus=True))
    for n, (pid1, _r) in enumerate(hop1):
        p = STORE.owners[pid1]
        f1 = STORE.filings.get(pid1) or {}
        svg.append(net_box(COL1, 24 + n * ROW, W1, pid1,
                           f1.get("corp_name") or p["name"],
                           p.get("state", NOT_LOOKED_UP), owner_sub(pid1)))
    for n, (pid2, _p, _k, _t) in enumerate(hop2):
        p = STORE.owners[pid2]
        f2 = STORE.filings.get(pid2) or {}
        svg.append(net_box(COL2, 24 + n * ROW, W2, pid2,
                           f2.get("corp_name") or p["name"],
                           p.get("state", NOT_LOOKED_UP), owner_sub(pid2)))

    notes = ["<p>This shows one entity and what sits up to two links away from it. Every line "
             "is labelled with the reason for it, because the reason is the claim</p>"]
    if nb["omitted1"]:
        notes.append("<p>%s more first-hop companies were left out to keep this readable. "
                     "The strongest links are drawn first</p>" % num(nb["omitted1"]))
    if nb["omitted2"]:
        notes.append("<p>%s more second-hop companies were left out</p>" % num(nb["omitted2"]))
    for kind, key, n in nb["hubs"]:
        notes.append(
            "<p>One link was withheld. The %s on this filing, %s, appears on %s other filings "
            "here. At that scale it is a hub, not a relationship, so it is reported as a count "
            "instead of drawn as lines</p>"
            % (e(EDGE_TEXT[kind].lower().replace("shared ", "")), e(title_case(key)), num(n)))

    return (
        "<div class=\"netwrap\"><div class=\"netscroll\">"
        "<svg viewBox=\"0 0 %d %d\" role=\"img\" aria-label=\"%s\">%s</svg>"
        "</div>%s<div class=\"netnote\">%s</div></div>"
        % (width, int(height), e(net_alt(oid, fl, hop1, hop2)), "".join(svg),
           edge_key(), "".join(notes))
    )

def net_alt(oid, fl, hop1, hop2):
    o = STORE.owners[oid]
    bits = ["Diagram: %s sits on the left." % (fl.get("corp_name") or o["name"])]
    for pid1, reasons in hop1:
        p = STORE.owners[pid1]
        f1 = STORE.filings.get(pid1) or {}
        bits.append("%s is linked by %s."
                    % (f1.get("corp_name") or p["name"],
                       " and ".join(sorted(set(r[1] for r in reasons)))))
    for pid2, parent, _k, t in hop2:
        p = STORE.owners[pid2]
        f2 = STORE.filings.get(pid2) or {}
        pp = STORE.owners[parent]
        bits.append("Two hops out, %s is linked to %s by %s."
                    % (f2.get("corp_name") or p["name"],
                       (STORE.filings.get(parent) or {}).get("corp_name") or pp["name"], t))
    return " ".join(bits)

def edge_key():
    return (
        "<div class=\"edgekey\">"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--officer\" /></svg>"
        "<span class=\"t\">Shared officer</span>"
        "<p>The same person is named on both filings. This is the strongest link here, and "
        "the one worth naming out loud</p></div>"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--mail\" /></svg>"
        "<span class=\"t\">Shared mailing address</span>"
        "<p>Both filings collect mail at the same address. Suggestive, and worth a second "
        "look, but shared suites happen</p></div>"
        "<div class=\"k\">"
        "<svg viewBox=\"0 0 78 10\" aria-hidden=\"true\">"
        "<line x1=\"0\" y1=\"5\" x2=\"78\" y2=\"5\" class=\"e-line e-line--agent\" /></svg>"
        "<span class=\"t\">Shared registered agent</span>"
        "<p>Both hired the same filing service. Weakest line on the diagram, drawn faint on "
        "purpose: these firms sign for thousands of unrelated companies</p></div>"
        "</div>")
