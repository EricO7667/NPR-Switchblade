# NPR Switchblade Tool

The **NPR Switchblade Tool** is an internal LOCAL MACHINE application built specifically for **Elan industries**.  
It is tightly coupled to the Elan industries inventory schema and **will not function correctly with external or generic inventory systems**.

The goal of this tool is to make **NPR (New Parts Report)** and **BOM (Bill of Materials)** processing **faster, easier, and more consistent**.

---

## Features

### AI MFGN Workspace Scrubber (Inventory-Based)

To speed up customer BOM validation against the Elan inventory, the tool scrubs **manufacturer part numbers (MFGNs)** and **descriptions** against the internal inventory in a listed workflow.

- Matches are displayed in a **panelized workspace UI**
- Multiple matches can be returned when an exact MFGN does not exist
- The system supports reviewing, editing, accepting, or rejecting matches before export

#### Workspace UI Layout

- **Upper Panel**
  - Displays BOM parts
  - Clickable entries to view matched inventory information

- **Middle Panel**
  - Edit part numbers
  - Accept or reject matches
  - Modify part descriptions prior to export

- **Lower Panel**
  - Displays all matched inventory parts
  - Shows stock levels and alternates under the XXX part number
  - Designed for review and approval of matched results

- **Top Toolbar**
  - Input selection
  - Workspace save/load (stored in database for quick resume)
  - NPR formatter/exporter
  - BOM formatter/exporter
  - Filtering tools

---

## Planned Features

### External Part Lookup

External lookup support has been a goal since the beginning of the project.

- Planned **Digi-Key API integration**
- Semi-automated alternate and substitute part discovery
- Intended to further reduce manual NPR and BOM processing time



---

## Notes

This tool is under active development and is intended for **internal use only**.
