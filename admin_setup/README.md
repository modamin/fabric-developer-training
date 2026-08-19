# Lab Environment Setup

Provisions Fabric workspaces and assigns students for a training lab, driven by a single roster CSV.

The notebook [lab-environment-setup.ipynb](lab-environment-setup.ipynb) does two things:

1. Creates one workspace per distinct `workspace_name`.
2. Assigns each `user_name` their `role` on the matching workspace.

## 1. Create the roster CSV

Create a plain text file named `roster.csv` with these exact columns:

```csv
workspace_name,user_name,role
lab-student-01,student01@contoso.com,Member
lab-student-02,student02@contoso.com,Member
lab-shared,student01@contoso.com,Viewer
lab-shared,student02@contoso.com,Viewer
```

- `workspace_name` — the workspace to create (repeat the name to add multiple users to it).
- `user_name` — the student's email / UPN.
- `role` — one of `Admin`, `Member`, `Contributor`, `Viewer`.

Tip: build it in Excel and **Save As > CSV (Comma delimited)**, or type it directly in a text editor.

## 2. Upload the roster to the notebook

1. Import `lab-environment-setup.ipynb` into Fabric (**New item > Import notebook**).
2. Open the notebook and expand **Resources** in the left pane.
3. Under **Built-in**, upload your `roster.csv`.

## 3. Run the notebook

1. In the **Configuration** cell, set:
   - `CAPACITY_ID` — **required**. Enter the Fabric capacity ID to assign to every workspace. The notebook stops if this is empty.
   - `CSV_PATH` — leave this as `"./builtin/roster.csv"` for the uploaded notebook resource.
   - Leave `APPLY_CHANGES = False` for a dry-run first, then set it to `True` to apply.
2. Run all cells. The final **Summary** cell reports created workspaces and flags any failures.

## Requirements

- Run as a Fabric admin with permission to create workspaces and assign them to the target capacity.
- A valid Fabric capacity ID is required.
- Every `user_name` must already exist in the tenant (Entra).
- Re-running is safe for role assignments; workspaces that already exist will report an error row rather than being recreated.
