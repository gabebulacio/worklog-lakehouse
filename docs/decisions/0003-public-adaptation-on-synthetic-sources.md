# 0003 · A public adaptation on synthetic sources

**Status:** accepted · 2026-10-14 · revised 2026-10-17: this record first described a clean-room rebuild from scratch; the code is in fact adapted from the production platform with Claude Code, then scrubbed

**Context.** The problems this project solves come from a platform I run in production on Microsoft Fabric: consolidating worklogs from two systems, resolving identities across domains and logins, keeping finance-facing keys stable, and comparing sold with logged hours. That work, its code and its data belong to an employer, and this repository is public.

**Decision.** Adapt the platform, with Claude Code, for a fictional company with synthetic, deterministic sources (`nb_land_synthetic`). The generator reproduces the *kinds* of source behaviour that make the problem hard: blank emails for deactivated users, mixed timestamp formats, legacy and external domains, swapped name logins, namesakes, overlapping API pages, late logging, edits and deletions. No name, identifier, client or number from the original is kept, and anything else that could identify the employer, its people or its clients was removed before publishing.

**Consequences.**
- The repository can be public, and anyone can run it end to end.
- The generator doubles as a test fixture: every quirk exists on purpose, on fixed people, so tests can assert how each one is handled.
- The narrative (what went wrong in production and how it was fixed) lives in the case study, described in general terms.
