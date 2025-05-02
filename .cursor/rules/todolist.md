# TODO List

- [x] Add email validation (simple regex) to client registration.
- [x] Modify Query 2 (Arrests per Area):
    - [x] Remove the "gebieden" (areas) parameter from the client GUI.
    - [x] Update server-side query processing to reflect the removal.
- [ ] Modify Query 3 (Arrests by Descent/Age/Sex):
    - [x] Server: Fetch distinct Descent Codes from the dataset.
    - [x] Server: Add the list of Descent Codes *and descriptions* to the metadata sent to the client.
    - [ ] Client: Replace the existing Descent input with a multi-select dropdown populated from metadata.
    - [ ] Client/Shared: Define the mapping for Descent Codes (e.g., B -> Black, W -> White, H -> Hispanic, A -> Other Asian, C -> Chinese, D -> Cambodian, F -> Filipino, G -> Guamanian, I -> Indian, J -> Japanese, K -> Korean, L -> Laotian, O -> Other, P -> Pacific Islander, S -> Samoan, U -> Hawaiian, V -> Vietnamese, Z -> Asian Indian, X -> Unknown).
    - [ ] Server: Update query processing logic to handle multiple Descent Codes.
    - [ ] Client: Modify the graph generation:
        - [ ] X-axis: Descent Code (using the mapped full names).
        - [ ] Y-axis: Count of arrests.
        - [ ] Remove the Age distribution part of the graph.
        - [ ] If both Male and Female checkboxes are selected, split each Descent bar into two segments (one for Male count, one for Female count).
        - [ ] If only one sex is selected, show a single bar per Descent Code. 