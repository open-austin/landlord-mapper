# ---------------------------------------------------------------------------
# CSS. The site ships in two skins and they share EVERY structural rule.
#
# A skin is a token block and nothing else: CSS_BASE below is the whole design
# and never mentions a colour or a typeface literally, only var(--...). So a
# skin cannot drift structurally from the other one, and adding a third would be
# one more token block.
#
#   --survey  the accent as a MARK: fills, strokes, dimension runs, large type.
#             Needs 3:1 (non-text / large-text), not 4.5:1.
#   --link    the accent as SMALL TEXT: body links, sort arrows, hit highlights,
#             and any fill that carries text on top of it. Needs 4.5:1. In the
#             field skin the two are the same blue; in the DSA skin they are not,
#             because brand red is 4.00:1 on brand paper and fails AA for text.
#   --display chrome type: labels, headings, buttons, chips, graph node names.
#   --mono    figures and raw compared strings, where column alignment and
#             character-by-character comparison are the whole point. Stays a
#             true monospace in BOTH skins -- Styrene B would misalign a
#             ranking table, and brand conformance is not worth that.
# ---------------------------------------------------------------------------
CSS_TOKENS_FIELD = r"""
:root {
  --paper:#E9EBE0; --paper-2:#F3F4EC; --paper-3:#E1E4D8;
  --ink:#171E1B; --ink-2:#58625B; --rule:#A7B0A4;
  --survey:#2E5FA3; --survey-w:#D6DFEE; --link:#2E5FA3;
  --ochre:#7D5F16; --oxide:#9E3226; --focus:#2E5FA3; --flag:#7D5F16;
  --mono: ui-monospace, "Cascadia Mono", "SF Mono", SFMono-Regular, Menlo,
          Consolas, "Liberation Mono", "Courier New", monospace;
  --serif: Charter, "Iowan Old Style", "Palatino Linotype", Palatino,
           Georgia, Cambria, "Times New Roman", serif;
  --display: var(--mono);
  --gut: 30px; --wrap: 74rem; --col: 40rem;
}
@media (prefers-color-scheme: dark) {
  :root {
    --paper:#101310; --paper-2:#191E19; --paper-3:#151A15;
    --ink:#E2E7DE; --ink-2:#97A296; --rule:#333B33;
    --survey:#86ADE8; --survey-w:#1C2A3B; --link:#86ADE8;
    --ochre:#DCB25E; --oxide:#E58A78; --focus:#A8C6F0; --flag:#DCB25E;
  }
}
:root[data-theme="dark"] {
  --paper:#101310; --paper-2:#191E19; --paper-3:#151A15;
  --ink:#E2E7DE; --ink-2:#97A296; --rule:#333B33;
  --survey:#86ADE8; --survey-w:#1C2A3B; --link:#86ADE8;
  --ochre:#DCB25E; --oxide:#E58A78; --focus:#A8C6F0; --flag:#DCB25E;
}
:root[data-theme="light"] {
  --paper:#E9EBE0; --paper-2:#F3F4EC; --paper-3:#E1E4D8;
  --ink:#171E1B; --ink-2:#58625B; --rule:#A7B0A4;
  --survey:#2E5FA3; --survey-w:#D6DFEE; --link:#2E5FA3;
  --ochre:#7D5F16; --oxide:#9E3226; --focus:#2E5FA3; --flag:#7D5F16;
}
"""

# Austin DSA. Every value is either a brand colour verbatim or a documented,
# hue-locked derivation of one -- see BRAND_NOTES below for the contrast
# measurements that forced each derivation.
CSS_TOKENS_DSA = r"""
:root {
  --paper:#f6f4f3; --paper-2:#ffffff; --paper-3:#ece8e7;
  --ink:#231f20; --ink-2:#605c5c; --rule:#8c8989;
  --survey:#ec1f27; --survey-w:#fbd2d4; --link:#c4151c;
  --ochre:#6d5300; --oxide:#a00a10; --focus:#c4151c; --flag:#ffe45e;
  --mono: ui-monospace, "Cascadia Mono", "SF Mono", SFMono-Regular, Menlo,
          Consolas, "Liberation Mono", "Courier New", monospace;
  /* Body prose is NOT Styrene. The brand names it --font-display and ships one
     weight, Regular: no bold file, no italic. Running text needs both, and a
     browser would have to synthesise them. So Styrene carries the display and
     chrome, and prose gets a grotesque that has real weights -- which is also
     what the Echo app does (display token vs the default sans). */
  --serif: "Helvetica Neue", Helvetica, Arial, system-ui, sans-serif;
  --display: "StyreneB", "ManifoldDSA", "Helvetica Neue", Helvetica, Arial,
             system-ui, sans-serif;
  --gut: 30px; --wrap: 74rem; --col: 40rem;
}
@media (prefers-color-scheme: dark) {
  :root {
    --paper:#191617; --paper-2:#231f20; --paper-3:#1e1a1b;
    --ink:#f6f4f3; --ink-2:#a9a4a4; --rule:#494545;
    --survey:#ec1f27; --survey-w:#3a1416; --link:#f5726f;
    --ochre:#e8c34a; --oxide:#f5726f; --focus:#f5726f; --flag:#ffe45e;
  }
}
:root[data-theme="dark"] {
  --paper:#191617; --paper-2:#231f20; --paper-3:#1e1a1b;
  --ink:#f6f4f3; --ink-2:#a9a4a4; --rule:#494545;
  --survey:#ec1f27; --survey-w:#3a1416; --link:#f5726f;
  --ochre:#e8c34a; --oxide:#f5726f; --focus:#f5726f; --flag:#ffe45e;
}
:root[data-theme="light"] {
  --paper:#f6f4f3; --paper-2:#ffffff; --paper-3:#ece8e7;
  --ink:#231f20; --ink-2:#605c5c; --rule:#8c8989;
  --survey:#ec1f27; --survey-w:#fbd2d4; --link:#c4151c;
  --ochre:#6d5300; --oxide:#a00a10; --focus:#c4151c; --flag:#ffe45e;
}
"""

CSS_BASE = r"""
@media (min-width: 46rem) { :root { --gut: 56px; } }

html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0;
  background: var(--paper); color: var(--ink);
  font-family: var(--serif);
  font-size: clamp(1rem, 0.96rem + 0.2vw, 1.09rem);
  line-height: 1.62; overflow-x: hidden;
}
*, *::before, *::after { box-sizing: border-box; }
img, svg, table { max-width: 100%; }
a { color: var(--link); text-decoration-thickness: 1px; text-underline-offset: 2px; }
:focus-visible { outline: 2px solid var(--focus); outline-offset: 3px; }

.m { font-family: var(--mono); font-variant-numeric: tabular-nums; }
.eyebrow {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2); line-height: 1.4;
}
.num { font-family: var(--mono); font-variant-numeric: tabular-nums; }
.wrap { max-width: var(--wrap); margin-inline: auto; padding-inline: clamp(1rem, 4vw, 3rem); }
.prose { max-width: var(--col); }
.band { padding-block: clamp(2.6rem, 7vw, 5rem); }
.band + .band { border-top: 1px solid var(--rule); }

.masthead { padding-block: clamp(1.4rem, 4vw, 2.4rem) clamp(2rem, 6vw, 3.4rem); }
.topline {
  display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between;
  gap: 1rem; border-bottom: 2px solid var(--ink); padding-bottom: 0.7rem;
}
.orgmark {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.2em;
  text-transform: uppercase; color: var(--ink);
}
.orgmark a { color: var(--ink); text-decoration: none; }
.orgmark a:hover { color: var(--link); }
.orgmark b { font-weight: 700; }
.orgmark span { color: var(--ink-2); }
.themebtn {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.12em;
  text-transform: uppercase; background: transparent; color: var(--ink-2);
  border: 1px solid var(--rule); padding: 0.4rem 0.7rem; cursor: pointer;
}
.themebtn:hover { color: var(--ink); border-color: var(--ink); }

h1 {
  font-family: var(--display); font-weight: 700; text-transform: uppercase;
  font-size: clamp(2.05rem, 8.4vw, 4.4rem); line-height: 0.94;
  letter-spacing: -0.035em; text-wrap: balance;
  margin-block: clamp(1.6rem, 5vw, 2.6rem) 0; max-width: 22ch;
}
h1 em { font-style: normal; color: var(--survey); }
.deck { max-width: 42rem; margin-top: 1.1rem; font-size: 1.06em; }

.stamp {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.11em;
  text-transform: uppercase; line-height: 1.5; color: var(--ochre);
  border: 2px solid var(--ochre); outline: 1px solid var(--ochre);
  outline-offset: 3px; padding: 0.6rem 0.85rem; max-width: 36rem; margin-top: 2rem;
}
@media (min-width: 52rem) { .stamp { transform: rotate(-0.9deg); transform-origin: left center; } }

.datestrip {
  display: flex; flex-wrap: wrap; gap: 0 1.6rem; margin-top: 1.8rem;
  padding-top: 0.9rem; border-top: 1px solid var(--rule); max-width: 52rem;
}
.datestrip div {
  font-family: var(--mono); font-size: 0.6875rem; letter-spacing: 0.06em;
  text-transform: uppercase; color: var(--ink-2); font-variant-numeric: tabular-nums;
}
.datestrip b { color: var(--ink); font-weight: 700; }

.lookup {
  background: var(--paper-2); border: 1px solid var(--rule);
  padding: clamp(1.1rem, 4vw, 1.8rem); max-width: 46rem;
}
.lookup label {
  display: block; font-family: var(--display); font-size: 0.6875rem;
  letter-spacing: 0.13em; text-transform: uppercase; color: var(--ink-2);
  margin-bottom: 0.55rem;
}
.field { display: flex; flex-wrap: wrap; gap: 0.6rem; }
.field input {
  flex: 1 1 16rem; min-width: 0; font-family: var(--mono); font-size: 1rem;
  letter-spacing: 0.01em; text-transform: uppercase; background: var(--paper);
  color: var(--ink); border: 2px solid var(--ink); padding: 0.7rem 0.75rem;
}
.field input::placeholder { color: var(--ink-2); text-transform: uppercase; }
.btn {
  font-family: var(--display); font-size: 0.8125rem; letter-spacing: 0.1em;
  text-transform: uppercase; font-weight: 700; background: var(--ink);
  color: var(--paper); border: 2px solid var(--ink); padding: 0.7rem 1.1rem;
  cursor: pointer;
}
.btn:hover { background: var(--survey); border-color: var(--survey); color: var(--paper); }
.btn-quiet { background: transparent; color: var(--ink); border: 1px solid var(--rule); font-weight: 400; }
.btn-quiet:hover { background: transparent; color: var(--link); border-color: var(--survey); }
.btn-off { opacity: 0.4; pointer-events: none; }
.scopenote { margin-top: 1rem; max-width: 44rem; font-size: 0.95em; color: var(--ink-2); }

/* the surveyor's dimension run: stroke style is the match state */
.chain {
  display: grid; grid-template-columns: var(--gut) minmax(0, 1fr);
  column-gap: clamp(0.8rem, 3vw, 1.5rem); row-gap: 0;
  border-top: 1px solid var(--ink); border-bottom: 1px solid var(--ink);
  max-width: 56rem;
}
.node { position: relative; }
.node .run {
  position: absolute; top: 0; bottom: 0; right: 0; width: 2px;
  background: linear-gradient(var(--survey), var(--survey));
}
.node--dashed .run {
  background: repeating-linear-gradient(180deg, var(--ochre) 0 5px, transparent 5px 11px);
}
.node--stop .run { background: linear-gradient(var(--ink), var(--ink)); }
.node .tick {
  position: absolute; top: 1.22em; right: 0; width: 100%; height: 1px; background: var(--rule);
}
.node .mark {
  position: absolute; top: calc(1.22em - 5px); right: -4px; width: 11px; height: 11px;
  background: var(--survey);
}
.node--dashed .mark { background: var(--paper); border: 2px solid var(--ochre); }
.node--stop .mark { background: var(--ink); }
.node--end .run { bottom: auto; height: 2.4em; }
.node--end::after {
  content: ""; position: absolute; top: 2.4em; right: -7px; width: 17px; height: 3px;
  background: var(--survey);
}
.node--stop.node--end::after { background: var(--ink); width: 23px; right: -10px; height: 4px; }
.node--dashed.node--end::after {
  background: none; border-bottom: 2px dashed var(--ochre); height: 0; width: 11px;
  right: -4px; opacity: 0.55;
}

.rec { padding-block: clamp(1.15rem, 3.4vw, 1.7rem); min-width: 0; }
.chain > .rec:not(:first-of-type) { border-top: 1px solid var(--rule); }
.rec h3 {
  font-family: var(--serif); font-size: 1.12em; font-weight: 600; line-height: 1.3;
  margin: 0.15rem 0 0.6rem;
}
.dl { display: grid; grid-template-columns: minmax(0, 1fr); gap: 0.55rem 1.4rem; margin: 0; }
@media (min-width: 34rem) { .dl--2 { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.dl > div { min-width: 0; }
.dl dt {
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2);
}
.dl dd {
  margin: 0.1rem 0 0; font-family: var(--mono); font-size: 0.875rem;
  font-variant-numeric: tabular-nums; line-height: 1.45; word-break: break-word;
}
.dl dd.raw { color: var(--ink-2); font-size: 0.8125rem; }
.dl dd .approx { color: var(--ochre); }
.srcstamp {
  margin-top: 1rem; padding-top: 0.5rem; border-top: 1px dotted var(--rule);
  font-family: var(--mono); font-size: 0.625rem; letter-spacing: 0.09em;
  text-transform: uppercase; color: var(--ink-2); font-variant-numeric: tabular-nums;
}
.tell {
  border-left: 3px solid var(--oxide); padding-left: 0.8rem; margin-top: 0.9rem;
  font-size: 0.95em; color: var(--ink); max-width: 38rem;
}
.tell--quiet { border-left-color: var(--rule); color: var(--ink-2); }

.matchcheck {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 0.3rem;
}
@media (min-width: 38rem) { .matchcheck { grid-template-columns: 1fr 1fr; } }
.matchcheck > div { background: var(--paper-2); padding: 0.7rem 0.8rem; }
.matchcheck .hd {
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2); display: block; margin-bottom: 0.3rem;
}
.matchcheck .val { font-family: var(--mono); font-size: 0.875rem; word-break: break-word; }
.matchcheck .val.hit { color: var(--link); font-weight: 700; }

.chip {
  display: inline-flex; align-items: center; gap: 0.4rem; font-family: var(--display);
  font-size: 0.625rem; letter-spacing: 0.13em; text-transform: uppercase;
  padding: 0.22rem 0.5rem; white-space: nowrap;
}
.chip--matched { background: var(--link); color: var(--paper); border: 1px solid var(--survey); }
.chip--norec {
  background: transparent; color: var(--ink); border: 1px solid var(--ink);
  box-shadow: 0 0 0 2px var(--paper), 0 0 0 3px var(--ink); margin-right: 3px;
}
.chip--unknown { background: transparent; color: var(--ochre); border: 1px dashed var(--ochre); }
.rechead { display: flex; flex-wrap: wrap; align-items: center; gap: 0.6rem; }

.payload {
  background: var(--survey-w); border: 2px solid var(--survey);
  padding: clamp(1rem, 3.5vw, 1.5rem); margin-top: 0.5rem;
}
.payload--stop { background: var(--paper-2); border-color: var(--ink); }
.payload--open { background: var(--paper-2); border: 2px dashed var(--ochre); }
.figs { display: flex; flex-wrap: wrap; gap: clamp(1.2rem, 5vw, 2.6rem); }
.fig { min-width: 0; }
.fig .v {
  display: block; font-family: var(--mono); font-weight: 700;
  font-variant-numeric: tabular-nums; font-size: clamp(1.6rem, 6vw, 2.6rem);
  line-height: 1; letter-spacing: -0.03em; color: var(--ink);
}
.fig .k {
  display: block; font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.15em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.4rem;
}
.fig .k .approx { color: var(--ochre); }
.payload .who {
  font-family: var(--display); font-size: 0.9375rem; font-weight: 700;
  word-break: break-word; margin-bottom: 0.9rem; display: block;
}

.legendband { background: var(--paper-3); }
.endings {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.6rem;
}
@media (min-width: 50rem) { .endings { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
.ending {
  background: var(--paper-2); padding: clamp(1rem, 3vw, 1.4rem);
  display: grid; grid-template-columns: 28px minmax(0, 1fr);
  column-gap: 0.9rem; row-gap: 0;
}
.ending .glyph { position: relative; height: 100%; min-height: 74px; }
.ending .body { min-width: 0; display: flex; flex-direction: column; gap: 0.55rem; }
.ending h3 {
  font-family: var(--display); font-size: 0.75rem; letter-spacing: 0.13em;
  text-transform: uppercase; margin: 0;
}
.ending p { margin: 0; font-size: 0.95em; }
.ending .ex {
  font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2);
  word-break: break-word; border-top: 1px dotted var(--rule); padding-top: 0.5rem;
}
.glyph .g-run { position: absolute; top: 4px; right: 8px; width: 2px; height: 44px; }
.glyph .g-mark { position: absolute; top: 0; right: 3px; width: 11px; height: 11px; }
.glyph .g-term { position: absolute; right: 0; }
.g--matched .g-run { background: var(--survey); height: 58px; }
.g--matched .g-mark { background: var(--survey); }
.g--matched .g-term { top: 62px; right: 1px; width: 17px; height: 3px; background: var(--survey); }
.g--norec .g-run { background: var(--ink); height: 44px; }
.g--norec .g-mark { background: var(--ink); }
.g--norec .g-term { top: 48px; right: -2px; width: 23px; height: 4px; background: var(--ink); }
.g--unknown .g-run {
  background: repeating-linear-gradient(180deg, var(--ochre) 0 5px, transparent 5px 11px);
  height: 52px; opacity: 0.75;
}
.g--unknown .g-mark { background: var(--paper-2); border: 2px solid var(--ochre); }
.g--unknown .g-term {
  top: 58px; right: 3px; width: 11px; height: 0;
  border-bottom: 2px dashed var(--ochre); opacity: 0.4;
}

.sharenote {
  display: flex; flex-wrap: wrap; align-items: baseline; gap: 0.7rem;
  margin-top: 1.4rem; max-width: 46rem;
}
.sharenote .big {
  font-family: var(--mono); font-weight: 700; font-size: 1.6rem;
  letter-spacing: -0.02em; color: var(--ink); font-variant-numeric: tabular-nums;
}
.sharenote p { margin: 0; font-size: 0.95em; max-width: 34rem; }

.empty {
  margin-top: 1.8rem; border: 1px solid var(--rule); border-left: 3px solid var(--ochre);
  background: var(--paper-2); padding: clamp(0.9rem, 3vw, 1.3rem); max-width: 40rem;
}
.empty h3 { font-family: var(--display); font-size: 0.8125rem; letter-spacing: 0.06em; margin: 0 0 0.5rem; }
.empty p { margin: 0 0 0.6rem; font-size: 0.95em; }
.empty p:last-child { margin-bottom: 0; }

.profhead { display: flex; flex-direction: column; gap: 0.9rem; align-items: flex-start; }
.profhead h2 {
  font-family: var(--display); font-weight: 700; text-transform: uppercase;
  font-size: clamp(1.35rem, 5.2vw, 2.5rem); line-height: 1.02;
  letter-spacing: -0.03em; margin: 0; word-break: break-word;
}
.profhead .alias { font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2); word-break: break-word; }

.headfigs {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.8rem;
}
@media (min-width: 44rem) { .headfigs { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.headfigs .cell { background: var(--paper-2); padding: clamp(0.9rem, 3vw, 1.3rem); }
.headfigs .cell--soft { background: var(--paper-3); }
.headfigs .v {
  display: block; font-family: var(--mono); font-weight: 700;
  font-variant-numeric: tabular-nums; font-size: clamp(1.4rem, 5vw, 2.1rem);
  line-height: 1; letter-spacing: -0.03em;
}
.headfigs .k {
  display: block; font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.14em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.45rem;
}
.headfigs .k .approx { color: var(--ochre); }

.subhead {
  font-family: var(--display); font-size: 0.75rem; letter-spacing: 0.16em;
  text-transform: uppercase; color: var(--ink); border-bottom: 2px solid var(--ink);
  padding-bottom: 0.4rem; margin: clamp(2rem, 6vw, 3rem) 0 0;
}
.tablescroll { overflow-x: auto; border: 1px solid var(--rule); border-top: 0; }
table {
  border-collapse: collapse; min-width: 46rem; width: 100%;
  font-family: var(--mono); font-variant-numeric: tabular-nums; font-size: 0.8125rem;
}
thead th {
  text-align: left; font-size: 0.625rem; letter-spacing: 0.11em; text-transform: uppercase;
  font-weight: 400; color: var(--ink-2); background: var(--paper-3);
  padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--rule); white-space: nowrap;
}
tbody td { padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--rule); white-space: nowrap; }
tbody tr:nth-child(even) td { background: var(--paper-3); }
tbody tr:hover td { background: var(--survey-w); }
td.r, th.r { text-align: right; }
tfoot td {
  padding: 0.65rem 0.75rem; font-weight: 700; border-top: 2px solid var(--ink);
  background: var(--paper-2); white-space: nowrap;
}
.cty {
  font-size: 0.625rem; letter-spacing: 0.09em; text-transform: uppercase;
  border: 1px solid var(--rule); padding: 0.1rem 0.35rem; color: var(--ink-2);
}
.tblnote {
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.08em;
  text-transform: uppercase; color: var(--ink-2); margin-top: 0.6rem;
}
.pager {
  display: flex; flex-wrap: wrap; align-items: center; gap: 0.7rem; margin-top: 1rem;
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.1em;
  text-transform: uppercase; color: var(--ink-2);
}
.pager a { text-decoration: none; }

.netwrap { border: 1px solid var(--rule); border-top: 0; background: var(--paper-2); }
.netscroll { overflow-x: auto; padding: clamp(0.6rem, 2vw, 1.1rem); }
.netscroll svg { min-width: 62rem; width: 100%; height: auto; display: block; }
.n-box { fill: var(--paper); stroke: var(--rule); stroke-width: 1; }
.n-box--focus { fill: var(--survey-w); stroke: var(--survey); stroke-width: 2; }
.n-name { fill: var(--ink); font-family: var(--display); font-size: 12.5px; font-weight: 700; letter-spacing: 0.01em; }
.n-state { fill: var(--ink-2); font-family: var(--display); font-size: 9.5px; letter-spacing: 0.11em; }
.n-link { text-decoration: none; }
.e-line { stroke: var(--ink-2); stroke-width: 1.6; fill: none; }
.e-line--officer { stroke: var(--survey); stroke-width: 2.2; }
.e-line--agent { stroke: var(--ink-2); stroke-width: 1.4; stroke-dasharray: 8 5; stroke-opacity: 0.6; }
.e-line--mail { stroke: var(--ink-2); stroke-width: 1.8; stroke-dasharray: 2 4.5; }
.e-knock { fill: var(--paper-2); }
.e-label { fill: var(--ink); font-family: var(--display); font-size: 10px; letter-spacing: 0.09em; }
.e-label--weak { fill: var(--ink-2); }
.sw-fill { fill: var(--survey); }
.sw-ink { fill: var(--ink); }
.sw-hollow { fill: none; stroke: var(--ochre); stroke-width: 2; }
.edgekey {
  border-top: 1px solid var(--rule); padding: clamp(0.9rem, 3vw, 1.3rem);
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 0.85rem;
}
@media (min-width: 44rem) { .edgekey { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
.edgekey .k { display: flex; flex-direction: column; gap: 0.35rem; min-width: 0; }
.edgekey .k .t { font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.11em; text-transform: uppercase; }
.edgekey .k p { margin: 0; font-size: 0.9em; color: var(--ink-2); }
.edgekey svg { display: block; height: 10px; width: 78px; min-width: 0; }
.netnote { padding: 0 clamp(0.9rem, 3vw, 1.3rem) clamp(0.9rem, 3vw, 1.3rem); }
.netnote p { margin: 0.4rem 0 0; font-size: 0.9em; color: var(--ink-2); max-width: 46rem; }

.foot { background: var(--paper-3); }
.footgrid { display: grid; grid-template-columns: minmax(0, 1fr); gap: 2rem; }
@media (min-width: 48rem) { .footgrid { grid-template-columns: 1.1fr 1fr; } }
.footgrid h3 {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.16em;
  text-transform: uppercase; color: var(--ink-2); margin: 0 0 0.7rem;
}
.footgrid p { margin: 0 0 0.7rem; font-size: 0.95em; max-width: 34rem; }
.srclist { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 0.5rem; }
.srclist li {
  font-family: var(--mono); font-size: 0.75rem; color: var(--ink-2);
  font-variant-numeric: tabular-nums;
}
.srclist b { color: var(--ink); font-weight: 700; }

@media (prefers-reduced-motion: no-preference) {
  .chain .node .run { transform-origin: top; animation: drawdown 620ms ease-out both; }
  .chain > .rec { animation: liftin 460ms ease-out both; }
  .chain > .rec:nth-of-type(1) { animation-delay: 90ms; }
  .chain > .rec:nth-of-type(2) { animation-delay: 190ms; }
  .chain > .rec:nth-of-type(3) { animation-delay: 290ms; }
  .chain > .rec:nth-of-type(4) { animation-delay: 390ms; }
  .chain > .rec:nth-of-type(5) { animation-delay: 490ms; }
  @keyframes drawdown { from { transform: scaleY(0); } to { transform: scaleY(1); } }
  @keyframes liftin { from { opacity: 0; transform: translateY(7px); } to { opacity: 1; transform: none; } }
}
/* filtered browse furniture. No new visual system: the stroke still carries
   the state, the mono/serif split and the paper palette are unchanged. */
.navmark { display: flex; flex-wrap: wrap; gap: 0.2rem 1.05rem; }
.navmark a {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.12em;
  text-transform: uppercase; color: var(--ink-2); text-decoration: none;
  padding-bottom: 2px; border-bottom: 2px solid transparent;
}
.navmark a:hover { color: var(--link); }
.navmark a[aria-current="page"] { color: var(--ink); border-bottom-color: var(--survey); }

.facets {
  background: var(--paper-2); border: 1px solid var(--rule);
  padding: clamp(1rem, 3.4vw, 1.6rem); display: grid;
  grid-template-columns: minmax(0, 1fr); gap: 1.1rem;
}
@media (min-width: 40rem) { .facets { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (min-width: 62rem) { .facets { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.facets .fset { display: flex; flex-direction: column; gap: 0.35rem; min-width: 0; }
.facets .fset--wide { grid-column: 1 / -1; }
.facets label, .facets .flab {
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.13em;
  text-transform: uppercase; color: var(--ink-2);
}
.facets input, .facets select {
  font-family: var(--mono); font-size: 0.8125rem; background: var(--paper);
  color: var(--ink); border: 1px solid var(--ink); padding: 0.45rem 0.5rem;
  min-width: 0; width: 100%;
}
.facets select[multiple] { height: 8.5rem; }
.facets .pair { display: flex; gap: 0.4rem; }
.facets .hint { font-size: 0.8em; color: var(--ink-2); font-family: var(--serif); }
.facets .go {
  grid-column: 1 / -1; display: flex; flex-wrap: wrap; gap: 0.7rem;
  align-items: center; border-top: 1px solid var(--rule); padding-top: 1rem;
}
.facets .go a.btn { text-decoration: none; display: inline-block; }

.countline {
  font-family: var(--mono); margin: 1.5rem 0 0; display: flex; flex-wrap: wrap;
  align-items: baseline; gap: 0.6rem;
}
.countline b {
  font-size: clamp(1.5rem, 5vw, 2.2rem); letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.countline span { font-size: 0.8125rem; color: var(--ink-2); max-width: 44rem; }
thead th a { color: var(--ink-2); text-decoration: none; white-space: nowrap; }
thead th a:hover { color: var(--link); }
thead th .sortmark { color: var(--link); font-weight: 700; }
td .rk {
  font-weight: 700; color: var(--ink-2); font-variant-numeric: tabular-nums;
}

.statebar {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.2rem;
}
@media (min-width: 40rem) { .statebar { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (min-width: 62rem) { .statebar { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
.statebar > div { background: var(--paper-2); padding: clamp(0.8rem, 2.6vw, 1.1rem); }
.statebar .v {
  display: block; font-family: var(--mono); font-weight: 700; line-height: 1;
  font-size: clamp(1.25rem, 4.4vw, 1.8rem); letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.statebar .k {
  display: block; font-family: var(--display); font-size: 0.625rem;
  letter-spacing: 0.13em; text-transform: uppercase; color: var(--ink-2);
  margin-top: 0.45rem;
}
.statebar p { margin: 0.55rem 0 0; font-size: 0.9em; }
.jobs {
  display: grid; grid-template-columns: minmax(0, 1fr); gap: 1px;
  background: var(--rule); border: 1px solid var(--rule); margin-top: 1.6rem;
}
@media (min-width: 40rem) {
  .jobs { grid-template-columns: repeat(auto-fit, minmax(15rem, 1fr)); }
}
.job {
  background: var(--paper-2); padding: clamp(1rem, 3vw, 1.35rem);
  display: flex; flex-direction: column; gap: 0.6rem; min-width: 0;
}
.job .ix {
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.18em;
  color: var(--ink-2); border-bottom: 1px solid var(--rule);
  padding-bottom: 0.45rem; font-variant-numeric: tabular-nums;
}
.job h3 {
  font-family: var(--display); font-size: 0.8125rem; letter-spacing: 0.03em;
  text-transform: uppercase; line-height: 1.35; margin: 0;
}
.job p { margin: 0; font-size: 0.93em; flex: 1 1 auto; }
.job a.go {
  font-family: var(--display); font-size: 0.6875rem; letter-spacing: 0.11em;
  text-transform: uppercase; text-decoration: none; align-self: flex-start;
  border-bottom: 2px solid var(--survey); padding-bottom: 2px;
}
.job a.go:hover { color: var(--ink); border-bottom-color: var(--ink); }
.skinswitch {
  margin: 1.1rem 0 0; padding-top: 0.8rem; border-top: 1px dotted var(--rule);
  font-family: var(--display); font-size: 0.625rem; letter-spacing: 0.11em;
  text-transform: uppercase;
}
.skiplink { position: absolute; left: -9999px; }
.skiplink:focus {
  left: 1rem; top: 1rem; z-index: 5; background: var(--ink); color: var(--paper);
  padding: 0.5rem 0.8rem; font-family: var(--display); font-size: 0.75rem;
}
"""

# Served from /brand/, cached immutable. font-display:swap so the page paints in
# the fallback grotesque immediately rather than blocking on 134 KB of Styrene --
# the fallback stack is metrically close enough that the swap is not a jolt.
# Styrene B stays an .otf because converting it to woff2 needs brotli, which is
# not in the stdlib and this image installs nothing.
CSS_DSA_FONTS = r"""
@font-face {
  font-family: "ManifoldDSA"; font-style: normal; font-weight: 400;
  font-display: swap;
  src: url("/brand/ManifoldDSA-Regular.woff2") format("woff2");
}
@font-face {
  font-family: "StyreneB"; font-style: normal; font-weight: 400;
  font-display: swap;
  src: url("/brand/StyreneB-Regular.otf") format("opentype");
}
"""

# What the DSA skin adds on top of the shared structure. Three things only: the
# chapter mark in the masthead, one red banner strip, and the handful of places
# where a face designed for display needs different metrics than a monospace.
CSS_DSA_EXTRA = r"""
/* Styrene B is a grotesque, not a typewriter face: at the same nominal size it
   sets wider and reads larger than the mono it replaces. These claw back the
   tracking that was tuned for monospace so the labels do not sprawl. */
.eyebrow, .dl dt, .matchcheck .hd, .fig .k, .headfigs .k, .statebar .k,
.tblnote, .edgekey .k .t, .footgrid h3, .job .ix { letter-spacing: 0.1em; }
.btn, .chip, .navmark a, .job h3, .job a.go { letter-spacing: 0.07em; }
body { font-size: clamp(0.98rem, 0.94rem + 0.2vw, 1.05rem); line-height: 1.58; }

/* Only StyreneB-Regular exists, so every font-weight:700 on a --display rule
   would be SYNTHESISED bold: the browser smears the Regular outline sideways,
   and on a face with this much character it reads as a rendering fault. These
   are the display rules that asked for 700; uppercase and tracking carry the
   emphasis instead. Rules that set 700 on --mono are left alone -- the
   monospace stack has a real bold, and the figures should keep it. */
h1, .profhead h2 { letter-spacing: -0.02em; font-weight: 400; }
.btn, .payload .who, .orgmark b { font-weight: 400; }

/* The mark. bat-circle-red carries its own red disc, so it reads on paper, on
   the red banner and on the dark ground without a variant per surface -- which
   is exactly the case the branding guide points it at. */
.orgmark { display: flex; align-items: center; gap: 0.6rem; }
.orgmark a { display: flex; align-items: center; gap: 0.6rem; }
.orgmark .wm { color: var(--ink); }
.batmark { width: 34px; height: 34px; flex: none; display: block; }

/* The signature: a full-bleed DSA-red banner under the topline, wordmark
   knocked out in white. This is bat-banner-left-red.svg rebuilt as live CSS so
   it reflows on a phone instead of scaling a fixed raster. */
.brandband { background: var(--survey); margin-top: 0.9rem; }
.brandband .wrap {
  display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1.1rem;
  padding-block: 0.55rem;
}
/* Literal #ffffff, not var(--paper): the banner is DSA red in BOTH themes,
   because the brand red does not have a dark-mode variant and inventing one
   would be the one thing the guide forbids. So the knockout has to be pinned to
   white -- --paper would follow the theme and turn near-black on red. */
.brandband p {
  margin: 0; color: #ffffff; font-family: var(--display);
  font-size: 0.6875rem; letter-spacing: 0.16em; text-transform: uppercase;
}
.brandband p.thin { color: #ffffff; opacity: 0.82; letter-spacing: 0.1em; }

/* The "we could not look this up" chip is the one place the brand's yellow
   earns its keep: as a FILL it takes rich-black text at 12.8:1, where the same
   yellow as ink would be 1.16:1 on paper and unreadable. */
.chip--unknown {
  background: var(--flag); color: #231f20; border: 1px solid var(--ink);
  border-style: dashed;
}
"""

CSS_FIELD = CSS_TOKENS_FIELD + CSS_BASE

CSS_DSA = CSS_DSA_FONTS + CSS_TOKENS_DSA + CSS_BASE + CSS_DSA_EXTRA

# Why each DSA token is the value it is. Contrast ratios are computed, not
# eyeballed: WCAG 2.1 relative luminance, AA needs 4.5:1 for text under 18.66px
# bold / 24px, and 3:1 for large text and non-text UI.
#
#   --survey  #ec1f27  DSA Red, verbatim. 4.00:1 on --paper: fine as a mark,
#                      fails as small text, hence --link.
#   --link    #c4151c  brand --color-primary-dark, verbatim. 5.52:1 on --paper.
#                      Echo uses this same pair the same way for links.
#   --ink     #231f20  Rich Black, verbatim. 14.87:1.
#   --paper   #f6f4f3  brand --color-paper, verbatim.
#   --ink-2   #605c5c  brand --color-secondary, verbatim. 6.02:1.
#   --rule    #8c8989  brand --color-gray-med, verbatim. 3.16:1, non-text only.
#   --oxide   #a00a10  brand --color-danger, verbatim. 7.52:1.
#   --survey-w #fbd2d4 brand --color-info, verbatim (the pale red wash).
#   --flag    #ffe45e  brand --color-warning, verbatim, used only as a FILL.
#   --ochre   #6d5300  DERIVED: #ffe45e darkened at locked hue until it passes
#                      as ink (6.63:1). The brand has no mid-tone caution ink
#                      and the dashed "unknown" strokes need one at 3:1+.
#   dark --link #f5726f  DERIVED: #ec1f27 lightened at locked hue; brand red is
#                      4.10:1 on the dark ground, this is 6.43:1.
#   dark --ochre #e8c34a DERIVED the same way from #ffe45e, 10.56:1.
#   dark --paper #191617 DERIVED: Rich Black darkened, so --paper-2 can BE
#                      #231f20 (a real brand surface) and still sit above it.
BRAND_NOTES = None

THEME_JS = r"""
(function () {
  var root = document.documentElement;
  var btn = document.getElementById("themebtn");
  if (!btn) return;
  function prefersDark() {
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function currentIsDark() {
    var set = root.getAttribute("data-theme");
    if (set === "dark") return true;
    if (set === "light") return false;
    return prefersDark();
  }
  function paint() {
    var dark = currentIsDark();
    btn.textContent = dark ? "Light mode" : "Dark mode";
    btn.setAttribute("aria-pressed", dark ? "true" : "false");
  }
  btn.addEventListener("click", function () {
    var next = currentIsDark() ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("lm-theme", next); } catch (err) {}
    paint();
  });
  try {
    var saved = localStorage.getItem("lm-theme");
    if (saved === "dark" || saved === "light") root.setAttribute("data-theme", saved);
  } catch (err) {}
  paint();
})();
"""
