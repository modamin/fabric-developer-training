# Module 1 — Bronze ingestion

**Goal:** land raw source data in the `bronze` schema exactly as it arrives — no
cleaning. Two ingestion styles, chosen for what each is good at:

- **Data pipeline** → the 60 flat monthly workforce-event extracts. Repeatable
  batch copy with a month-offset loop.
- **Notebook** → the nested JSON pay-grid feed, FX, and reference masters. A
  pipeline Copy activity flattens nested arrays poorly; a notebook does it in a
  few lines.

---

The event files are named `workforce_events_YYYY-MM.csv`. The setup notebook owns
the watermark state: it creates the control table, seeds it on a first run, and
returns the current watermark. The discovery notebook then takes that watermark
as an input parameter, lists the source files on GitHub, and returns the array of
new files to copy. The pipeline just loops over that array with a plain Copy
activity, then advances the watermark after every Copy succeeds. No pipeline-side
date arithmetic.

## 1. Create the pipeline

1. Import these notebooks into the workspace and attach `lh_meridian_hr` as the
   **default lakehouse for each**:
   - `notebooks/nb_setup_lakehouse.ipynb` — pre-creates the target table,
     creates and seeds the watermark, and returns the current watermark.
   - `notebooks/nb_list_event_files.ipynb` — takes the watermark as input,
     lists the GitHub files, and returns the new ones.
   - `notebooks/nb_update_watermark.ipynb` — advances the watermark.
2. Delete or drop the table `workforce_events_raw` table from the lakehouse `lh_meridian_hr` if it already exists there. 
3. In all three notebooks, mark Cell 2 as the **parameter cell** so the pipeline
   can override the values.
4. In your lab workspace: **+ New item → Data pipeline**, name it
   `pl_ingest_events`.
5. Add a **Notebook** activity named `SetupTargetTable` and select
   `nb_setup_lakehouse`. It exits the watermark the next activity consumes:
   ```json
   {
     "pipeline_name": "workforce_events",
     "watermark": "2020-12-01 00:00:00"
   }
   ```

## 2. List the new files in a notebook

1. Add a **Notebook** activity named `ListNewFiles`, select
   `nb_list_event_files`, and connect it after `SetupTargetTable` with an
   **On success** dependency.
2. Under **Base parameters**, set `watermark` to the value the setup activity
   returned:
   ```text
   @json(activity('SetupTargetTable').output.result.exitValue).watermark
   ```
3. The notebook lists the files through the GitHub Contents API, keeps only months
   later than the watermark (capped at 60), and exits this JSON:
   ```json
   {
     "files": ["events/workforce_events_2021-01.csv", "..."],
     "watermark": "2025-12-01 00:00:00",
     "count": 60
   }
   ```
   - `files` — the relative Copy paths the ForEach iterates over.
   - `watermark` — the month to store after the batch succeeds.
   - `count` — how many files were selected (0 when already current).

## 3. Loop over the returned files

1. Add a **ForEach** activity named `ForEachFile` after `ListNewFiles`.
2. Select **Settings** and set **Items** to the `files` array from the notebook
   exit value:
   ```text
   @json(activity('ListNewFiles').output.result.exitValue).files
   ```
3. Leave **Sequential** unchecked. `SetupTargetTable` already created the target
   table, so parallel Copy activities append instead of racing to create it.
   When `files` is empty the loop simply does nothing.

## 4. Copy activity inside the loop

1. Inside `ForEachFile`, add **Copy data** → `CopyFile`.
2. **Source**: create an HTTP connection with authentication set to
   **Anonymous**. Enter the following literal value in the connection **URL**;
   make sure it ends with `/`:
   ```text
   https://raw.githubusercontent.com/modamin/fabric-developer-training/main/data/
   ```
   
   Set the relative URL in the Copy activity to the current item — the notebook
   already returns the correct path, so no expression building is needed:
   ```text
   @item()
   ```
   
   Switch the **File Format** to `Delimited Text`.
3. **Destination**: select Lakehouse `lh_meridian_hr`, choose **Tables**, click the check box `Enter manually`. Enter `bronze` for schema and `workforce_events_raw` for table name
   the table action to **Append**. The pipeline writes directly to the Delta
   table `bronze.workforce_events_raw`.

## 5. Update the watermark after the batch

1. Add a **Notebook** activity named `UpdateWatermark` after `ForEachFile` with
   an **On success** dependency. Select `nb_update_watermark`.
2. Under **Base parameters**, set:
   - `pipeline_name` = `workforce_events`.
   - `watermark_timestamp` = the `watermark` value the discovery notebook
     returned:
     ```text
     @json(activity('ListNewFiles').output.result.exitValue).watermark
     ```

When no new files were found, the notebook returns the existing watermark, so
this update is harmless. If any Copy fails, `UpdateWatermark` does not run and
the failed batch is retried from the same watermark.

## 6. Run and verify

You run this pipeline three times to see incremental loading end to end. At the
start only the months through **2025-09** are published; the instructor releases
the final three files (`2025-10`, `2025-11`, `2025-12`) between runs.

> **Instructor only:** the last three months must be parked before the session
> starts — see [instructor.md](instructor.md). Students: nothing to do here.

1. **Initial run.** `SetupTargetTable` seeds and returns December 2020, so
   `ListNewFiles` returns every published file and the loop copies them through
   **2025-09**. Query
   `bronze.ingestion_watermark` and confirm `watermark_timestamp` is
   `2025-09-01 00:00:00`.
   
   > **Instructor only:** restore the three parked files now, before anyone
   > starts the incremental run — see [instructor.md](instructor.md).
   
2. **Incremental run.** After the instructor publishes the last three files,
   run the pipeline again. `ListNewFiles` now returns only `2025-10`, `2025-11`,
   and `2025-12`, so the loop runs exactly **three** Copy activities and the
   watermark advances to `2025-12-01 00:00:00`.
3. **No-op run.** Run the pipeline a third time. With the watermark at the
   latest month, `ListNewFiles` returns zero files, the loop does nothing, and
   the watermark stays at `2025-12-01 00:00:00`.
4. Now open **Tables → bronze → workforce_events_raw** and confirm the Delta
   table contains approximately **124,485** rows.
