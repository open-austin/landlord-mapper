# Austin DSA brand assets

`bat-circle-red.svg` is the chapter mark: a black bat clutching the DSA rose on a
red disc. The bat is Austin's own, for the colony under the Congress Avenue
Bridge; the rose is national DSA's. It is used here as the masthead mark and the
favicon of the `dsa` skin.

## The two brand typefaces are deliberately not in this repo

The `dsa` skin asks for **Styrene B** and falls back to **Manifold DSA**, DSA
national's brand face. Neither font file is committed, because both are licensed
third-party software and this repository is public. A web font licence generally
covers serving the file from named domains, not redistributing it in a source
tree, and that is not a thing to get wrong on someone else's behalf.

So the skin ships without them and **degrades on purpose**: `--display` falls
back to Helvetica / Arial / `system-ui`, and `/brand/<font>` answers 404 rather
than failing the request. The layout, colour, contrast and mark are all correct
without the fonts. Only the letterforms are the fallback.

## To render it in the real chapter type

Drop these two files into this directory:

```
brand/StyreneB-Regular.otf          134 KB
brand/ManifoldDSA-Regular.woff2      22 KB
```

They live in the chapter's website repository under
`tools/static/fonts/`, wired up the same way there. `server.py` needs no change:
`BRAND_FILES` already lists both names, and `Dockerfile` already copies this
whole directory, so they are picked up as soon as they are present.

Confirm the licence covers the domain you are serving from before deploying them
anywhere public.

## Why only Regular

There is one weight of Styrene B, and no italic. That is why the skin puts body
prose in a grotesque that has real weights instead, and pins the display
headings to weight 400: a browser asked for bold it does not have will smear the
Regular outline sideways, and on a face with this much character that reads as a
rendering fault rather than as emphasis. See `CSS_DSA_EXTRA` in `server.py`.
