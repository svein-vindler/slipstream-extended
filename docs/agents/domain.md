# Domain docs

## Layout and consumer rules

This repository uses a single context: GLOSSARY.md at the root and architecture decision records in docs/adr/.

Before exploring domain behaviour, read the existing glossary and ADRs relevant to the task. If GLOSSARY-MAP.md exists in a future layout, follow its pointers for the relevant context.

If a glossary or ADR directory does not exist, proceed without treating the absence as a blocker or proposing empty scaffolding. The domain-modeling skill records terminology and decisions lazily when they are actually resolved and the task authorizes documentation changes.

## Vocabulary

Use defined glossary terms in issue titles, hypotheses, tests and design proposals. If a required concept is missing, distinguish a genuine terminology gap from an unnecessary new synonym before proposing a glossary change.

## Decisions

Surface conflicts with existing ADRs explicitly. A proposal may recommend revisiting a decision, but does not silently replace it. Keep private deployment details and operational evidence out of portable glossary and ADR content.
