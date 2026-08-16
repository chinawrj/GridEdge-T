# GridEdge Python Web

This is the reactive browser layer for GridEdge-T. It never writes SQLite.
All state-changing commands go through the authenticated local Rust API, which
retains the single ledger-writer boundary.

Run the Rust core on port 8790 and the Python web layer on port 8787 with the
same `GRIDEDGE_API_TOKEN`. The browser connects only to the Python layer.
