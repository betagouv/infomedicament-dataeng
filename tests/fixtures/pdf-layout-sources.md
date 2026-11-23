# EMA layout regression PDFs

The excerpts retain original page content, including bitmap resources, native
text overlays and drawing rules. Page references are one-based in the source
PDFs. They were selected from the existing EMA cache on 2026-11-20.

- `afqlir-rcp-tables.pdf`: source pages **30, 31, 22, 51**, in that order, from
  [Afqlir French product information](https://www.ema.europa.eu/fr/documents/product-information/afqlir-epar-product-information_fr.pdf).
  CIS **63409366** supplied the original report. Pages 30-31 cover pictograms,
  instructions and a French text overlay on an English bitmap. Pages 22 and 51
  cover composite graphs, labels extending outside bitmaps and rotated axes.
- `tecentriq-rcp-tables.pdf`: source pages **4, 42, 64**, in that order, from
  [Tecentriq French product information](https://www.ema.europa.eu/fr/documents/product-information/tecentriq-epar-product-information_fr.pdf).
  CIS **60345905** supplied the original report. The excerpts cover row/column
  spans, partially ruled statistical tables, shaded headers and shared estimates
  centered across treatment-arm columns.

The tests exercise extraction through final semantic HTML. They check cell
ownership, numeric alignment, exact shared values, composed figure pixels and
source-word/number preservation (visible text plus image alternative text).
Only page-number footers are intentionally omitted from that comparison.
