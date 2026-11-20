# RCP image and table diagnosis: CIS 63409366 and 60345905

Date: 2026-11-20. The initial inspection is recorded below; the subsequently authorized implementation is described in the follow-up section. No database rows or S3 objects were changed.

## Reproduction

Read the two specialties from `ansm_specialite`, their source PDFs from the existing EMA S3 cache, and their stored RCP HTML from PostgreSQL. Parsed the cached PDFs locally through the existing `parse_pdf` and `match_presentation` functions. For both specialties, the ordered table text from the local result exactly matches the stored RCP's ordered table text. The observed table problems therefore reproduce in the current extraction path; this is not merely an unverified frontend styling hypothesis.

Temporary inspection artifacts are under `/tmp/cis-pdf-diagnosis/` (not committed). These include the source PDFs, stored/generated HTML, page/table/image inventories, and rendered source pages. Poppler rendered Afqlir successfully, but reported font configuration errors on Tecentriq; the inspected Tecentriq renders were regenerated with PyMuPDF.

| CIS | Specialty | Cached PDF | Matched RCP output |
| --- | --- | --- | --- |
| 63409366 | AFQLIR 40 mg/mL, solution injectable en seringue preremplie | `afqlir-epar-product-information_fr.pdf`, 101 pages | 14 tables, 43 images, zero images within tables |
| 60345905 | TECENTRIQ 1200 mg, solution a diluer pour perfusion | `tecentriq-epar-product-information_fr.pdf`, 193 pages | 19 tables, 22 images, zero images within tables |

Page numbers below are one-based PDF page numbers.

## Confirmed Afqlir problems

1. **Cell ownership is lost.** On page 30, the first table has four pictograms in its left column. Page 31 has illustrated preparation steps in a three-column table. Four image placements on page 31 fall entirely within the detected table. Nevertheless, images are emitted as independent `Image` elements and the HTML builder appends them as sibling figures. `_table_to_html` has no image input. Sorting siblings by vertical position cannot place them back inside cells.
2. **Raw bitmap extraction changes the visible language of a figure.** The large syringe diagram on page 30 is embedded image xref 565. Its raw bitmap contains English labels (`Syringe cap`, `Finger grip`, etc.), while the displayed PDF page shows French labels (`Capuchon de la seringue`, `Ailette`, etc.). The PDF composes translated labels over the embedded bitmap. Exporting the bitmap alone exposes the underlying English version. A correctly bounded rendered page crop preserves the French composition; extracting another bitmap format cannot solve this.
3. **A diagram is misidentified as a table.** The label area below that syringe illustration is detected as an additional 2-by-3 table, containing `Butee en caoutchouc` and `Raccord Luer lock`. Those labels belong to the figure, not a standalone table.
4. **Mask loss is not established.** Although several page-30 images reference soft masks, the installed PyMuPDF 1.28.2 returns PNG data with alpha channels for the inspected images. The initial hypothesis that simply omitting an explicit mask-composition step causes these examples is not supported. Adding masks indiscriminately would be incorrect (the decoded image already has alpha).

## Confirmed Tecentriq problems

On page 42, source Tableau 9 has four logical columns: the efficacy measure and treatment arms A, B and C. Default `find_tables()` produces a 10-row, 12-column grid. The renderer writes all 12 slots as individual `<td>` elements, including empty placeholders. Entire groups of efficacy measurements across the treatment arms are collapsed into single cells in the default extraction. Paragraph-like footnote text below the main table is also detected as a second 3-by-3 table.

Other source tables have the same broad difficulty: page 64 is detected as an 8-by-9 grid despite its simpler logical column structure. This requires further page-by-page validation rather than assuming every unusual grid is wrong.

The renderer compounds extraction problems by collapsing all cell whitespace, including newlines, and discarding merged-cell structure, headings and inline formatting. The shared semantic sanitizer already permits `rowspan`, `colspan`, `th`, `br`, and images; this information is lost earlier in the PDF-specific path.

## Controlled probe, not a proposed hardcoded fix

On Tecentriq page 42, restricting extraction to the main table, disabling layout-assisted detection (`use_layout=False`), and supplying four manually guided columns produced a **6-row, 4-column** table with treatment-arm text assigned to separate cells. The probe used `vertical_strategy='explicit'`, vertical boundaries `[71, 250, 360, 470, 548]`, `horizontal_strategy='lines'`, and clip `(70, 81, 549, 466)`.

This shows the source text and coordinates can support materially better extraction without ML or a library replacement. It does not solve automatic boundary detection, subdivision of multiline statistical rows, merged section headings, or corpus-wide behavior. Manual page-specific coordinates must not become the production solution. Simply switching globally to `text` or `lines_strict` did not solve this page: the former absorbed non-table text and the latter detected no tables.

## Recommended implementation direction

1. Preserve full table/cell and figure bounding boxes through extraction; classify ownership before discarding body text or writing HTML.
2. Reconstruct table cells with image children, spans and structured cell content. Keep images that belong to cells out of the sibling figure stream.
3. Render composite figure regions where their displayed appearance differs from raw embedded images; include translation overlays and suppress duplicate exported labels. Region selection and accessible text still need deliberate handling.
4. Develop table strategies for partially ruled statistical tables, using geometry and aligned text; reject footnote and figure-label false positives. Evaluate Camelot only on cases still failing after this work, rather than replacing the whole pipeline first.
5. Use the actual Afqlir pages 30-31 and Tecentriq page 42 as regression fixtures, with visual comparison as well as cell ownership/text assertions. Keep a wider corpus check to avoid tuning solely to these two products.

The initial pass stopped at diagnosis. The user subsequently authorized the implementation below.

## Implementation follow-up

Implemented using the existing PyMuPDF dependency:

- Retain structured table cells, row/column spans, inline emphasis and line breaks through the semantic renderer; register cell images through the existing content-addressed image path.
- Ignore large filled text backgrounds when extracting rules. Recover missing vertical dividers from repeated horizontal-rule joints in partially ruled tables. No CIS names, page numbers or product-specific coordinates are embedded in production code.
- Split statistical measurement groups at shared native text baselines so each label retains its own treatment-arm values. Assign native lines once, rather than clipping characters independently for each cell. Shared estimates spanning arm columns become merged cells.
- Render displayed figure regions at 144 DPI, including translation overlays. Merge overlapping bitmap fragments and expand regions to complete associated labels, including adjacent rotated axis labels. Use the same native-line ownership for suppression and alternative text. Constrain table-image crops to cell interiors to avoid copying border fragments into images.
- Emit source display widths for responsive figure sizing. Preserve whitespace between formatted runs, and exclude table borders from underline inference inside cells.

Regression fixtures retain Afqlir source pages 30, 31, 22, 51 and Tecentriq pages 4, 42, 64. Nine layout tests cover cell ownership, composed figure pixels, label bounds, correct treatment-arm values, shared hazard ratios/p-values, spans and exact source-word/number counts (including image alternative text, excluding page footers). Existing Abasaglar coverage remains intact.

Validation: **259 tests passed**, Ruff lint and format checks passed, and `git diff --check` passed. HTML comparisons were inspected in a browser for Afqlir pages 22/30/31 and Tecentriq pages 4/42/64; the complete page-51 figure was inspected separately. Comparison artifacts are in `tmp/rcp-review/` and are self-contained HTML, using a simple review stylesheet rather than the application's production CSS.

Independent review caught and verified fixes for character duplication across column boundaries and clipped overlay labels. Whole-PDF parsing retains both RCP presentations in each source PDF, with the same presentation matching boundary. Stored RCPs and their existing images still require reprocessing before consumers see the new output.

Single local before/after measurements around `parse_pdf`, after imports, using identical PDF bytes and the code at `HEAD` as the baseline:

| Complete PDF | Before | After |
| --- | --- | --- |
| Abasaglar, 107 pages | 4.22 s | 4.77 s |
| Afqlir, 101 pages | 5.85 s | 6.48 s |
| Tecentriq, 193 pages | 11.52 s | 12.14 s |

These are local single-run measurements, not production capacity estimates. The added processing remains CPU-only with no new dependencies or model artifacts.

Limits: these are geometric heuristics for born-digital PDFs, not universal recovery of scanned or borderless tables. Pure vector-only figures without an associated bitmap are not addressed. Output image hashes and sequential HTML block IDs can change upon reprocessing. Visual validation covers the seven regression pages, not every page in the corpus.

## Code references

- [Extraction models and table rendering](../../src/infomedicament_dataeng/centralise/extract.py)
- [Sibling table/image dispatch and figure HTML](../../src/infomedicament_dataeng/centralise/parser.py)
- [Shared semantic sanitization](../../src/infomedicament_dataeng/parsing/semantic_parser.py)
