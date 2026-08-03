## Why I'm doing:

## What I'm doing:

Fixes #issue

## What type of PR is this:

- [ ] BugFix
- [ ] Feature
- [ ] Enhancement
- [ ] Refactor
- [ ] UT
- [ ] Doc
- [ ] Tool

Does this PR entail a change in behavior?

- [x] Yes, this PR will result in a change in behavior.
- [ ] No, this PR will not result in a change in behavior.

If yes, please specify the type of change:

- [ ] Interface/UI changes: syntax, type conversion, expression evaluation, display information
- [ ] Parameter changes: default values, similar parameters but with different default values
- [ ] Policy changes: use new policy to replace old one, functionality automatically enabled
- [ ] Feature removed
- [ ] Miscellaneous: upgrade & downgrade compatibility, etc.

## Checklist:

- [ ] I have added test cases for my bug fix or my new feature
- [ ] This pr needs user documentation (for new or modified features or behaviors)
  - [ ] I have added documentation for my new feature or new function
  - [ ] This pr needs auto generate documentation
- [ ] This is a backport pr

## Backport:
Backport targets are computed automatically from the PR type and per-branch metadata
(`.github/.status` / `.github/.lts`): BugFix/Doc/UT → recent releases ∪ recent LTS,
Enhancement → recent releases, others → none. No manual version checkboxes.
- To exclude a branch: comment `/no-backport branch-X` before merge (`/no-backport` for none).
- To add a branch beyond the computed set: comment `@Mergifyio backport branch-X` after merge.
