# TODO List

- [x] Add email validation (simple regex) to client registration.
- [x] Modify Query 2 (Arrests per Area):
    - [x] Remove the "gebieden" (areas) parameter from the client GUI.
    - [x] Update server-side query processing to reflect the removal.
- [x] Modify Query 3 (Arrests by Descent/Age/Sex):
    - [x] Server: Fetch distinct Descent Codes from the dataset.
    - [x] Server: Add the list of Descent Codes *and descriptions* to the metadata sent to the client.
    - [x] Client: Replace the existing Descent input with a multi-select dropdown populated from metadata.
    - [x] Client/Shared: Define the mapping for Descent Codes (added to constants.py).
    - [x] Server: Update query processing logic to handle multiple Descent Codes (already uses .isin()).
    - [x] Client: Modify the graph generation:
        - [x] X-axis: Descent Code (using the mapped full names).
        - [x] Y-axis: Count of arrests.
        - [x] Remove the Age distribution part of the graph.
        - [x] If both Male and Female checkboxes are selected, split each Descent bar into two segments (one for Male count, one for Female count).
        - [x] If only one sex is selected, show a single bar per Descent Code. 