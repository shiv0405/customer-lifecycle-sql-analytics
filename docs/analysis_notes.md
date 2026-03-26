# Analysis Notes

This project is intentionally written as an analyst-facing warehouse workflow rather than a modeling project. The emphasis is on:

- choosing the right grain for analysis
- defining business metrics clearly
- building reusable marts instead of one-off queries
- showing how SQL outputs translate into business recommendations
- keeping enough business context around the marts that a stakeholder handoff still makes sense
- separating quality checks, business questions, and final outputs instead of mixing everything into one query layer

The generated findings in `artifacts/analysis_findings.md` are meant to read like a concise analyst handoff or business review note.
