# Push checklist — 4 Oct 2026

Everything below is already done in the working tree **except the deletions**,
which need `git rm` because the files are tracked. Nothing here touches git
history. No force-push, no rewrite.

---

## Already changed on disk — review, then stage

| file | what changed |
|---|---|
| `README.md` | New opening: project renamed to **Storage Price Observatory**, size explained as the point, `--depth 1` clone, a worked `verify_day.py` example, a dated "How this got here" section, a verified seven-brand coverage table. Stale GitHub Actions / Netlify / "3,500 Public Storage facilities" claims corrected throughout. |
| `verify_day.py` | **New file.** 50 lines. Diffs two raw snapshots on `(store_id, sku)`. This is the thing that makes the 3 GB an argument instead of a liability. |
| `.github/workflows/*.yml` (7 files) | Seven-line RETIRED header on each, pointing at `collect_all.ps1` and the README. No workflow logic touched. |
| `collect_all.ps1` | One stale comment fixed — `-IncludePublicStorage` no longer says "CI does this already". |

## Still to do — these need you

```
git rm extraspace_scraper.py extraspace_parser.py test_extraspace.py
git rm extraspace_scraper.py.metadata.json extraspace_parser.py.metadata.json test_extraspace.py.metadata.json
git rm extraspace_locations.json
git rm docker-compose.yml
```

Backups of all of these are in your outputs folder under
`removed_from_storageDir_2026-10-04/` in case you want them back.

**Why `extraspace_*` goes:** its module docstring explains using curl_cffi's
native Chrome TLS cipher ordering to eliminate JA3 signature mismatches and
avoid tripping anti-bot WAFs. It is the most quotable thing in the repository
and it argues the opposite of your stated posture. Extra Space was never run —
their terms forbid automated collection and PerimeterX refuses the session
anyway. Nothing imports it; `daily_scraper.py` line 174 mentions it only in a
comment.

**Why `docker-compose.yml` goes:** a leftover WordPress + MySQL stack with
placeholder passwords. Unrelated to this project, and "why does a storage
scraper have a WordPress container" is a question you don't want to field.

## Then

```
git add README.md verify_day.py collect_all.ps1 .github/workflows
git commit
git push
```

Explicit paths only. Check `git status` before committing — `history/` will
have today's snapshots in it, and that is fine, but you should see what is
going in.

---

## Decide separately, not tonight

**`history/combined/` is 267 MB** and your own `.gitignore` comment calls it
"a rebuildable merge of every brand's freshest snapshot… the per-brand
snapshots are the record; this is derived." You excluded `latest.json` from it
but the rest is still tracked. If you want future growth to slow, one line in
`.gitignore` plus `git rm -r --cached history/combined` stops new versions from
being committed. **It does not shrink `.git`** — nothing shrinks `.git` without
rewriting history, which you should not do.

## Do not do

**Do not rewrite git history to shrink the repository.** It force-pushes,
rewrites every commit, breaks any clone that already exists, and is
unrecoverable if it goes wrong. The daily commit history is the most credible
artifact in this repository — it is the proof the pipeline actually ran. Do not
put it at risk to save disk space nobody is billing you for.

**Do not build a clean single-brand repository for the HN post.** A repository
whose history starts today is indistinguishable from a weekend project.

---

## Verified during this pass

- No employment or insider language anywhere in the repo — no "I work",
  no "my store", no "the team I was on". Public Storage appears only as a
  tracked brand.
- No secrets. The only password-shaped strings were the placeholders in the
  `docker-compose.yml` being removed.
- `private/` is gitignored, so the 25 daily run logs are not published.
- Snapshot coverage checked day by day: six of seven brands have captured every
  calendar day since they started; `independent` is missing 2026-09-11 and
  2026-09-26.
- `verify_day.py` was run on `publicstorage 2026-09-22 2026-09-23` and returned
  46,023 price changes, 23,282 up / 22,741 down — matching the independent
  reconstruction in `portfolio/STORAGE_PIPELINE_FINDING.md` exactly. The README
  example is real output, not illustrative.

## Not checked — yours to run

**Commit history and commit messages.** I cannot run git in your repos. Before
this goes public, skim the log for anything that shouldn't be there. The
GAScout history is the reason to bother.
