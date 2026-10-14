# 0003 · Clean-room rebuild on synthetic sources

**Status:** accepted · 2026-10-14

**Context.** The problems this project solves come from real work: consolidating worklogs from two systems, resolving identities across domains and logins, keeping finance-facing keys stable, and comparing sold with logged hours. That work, its code and its data belong to an employer, and this repository is public.

**Decision.** Rebuild from scratch on a fictional company with synthetic, deterministic sources (`nb_land_synthetic`). The generator reproduces the *kinds* of source behaviour that make the problem hard: blank emails for deactivated users, mixed timestamp formats, legacy and external domains, swapped name logins, namesakes, overlapping API pages, late logging, edits and deletions. No code, schema, name, identifier, client or number is copied from the original.

**Consequences.**
- The repository can be public, and anyone can run it end to end.
- The generator doubles as a test fixture: every quirk exists on purpose, on fixed people, so tests can assert how each one is handled.
- The narrative (what went wrong in production and how it was fixed) lives in the case study, described in general terms.
