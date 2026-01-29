## Why I'm doing:

The test cases for the `regexp_position` function in `test/sql/test_function/R/test_regex` were failing intermittently because the expected error message format was too specific. When invalid regex expressions are provided, the actual error message sometimes includes backend-specific information (e.g., `backend [id=10004]`), but the test on line 313 was expecting an exact match without this information.

This caused unstable tests that would pass or fail depending on which backend handled the query and how the error message was formatted.

## What I'm doing:

Updated line 313 in `test/sql/test_function/R/test_regex` to use a regex pattern `[REGEX].*Invalid regex expression: \(unclosed.*` instead of expecting an exact error message match. This pattern will successfully match error messages both with and without backend-specific details.

The change aligns with the existing error checking patterns used in lines 424, 428, 432, 436, 440, 444, 548, 552, 556, and 568 of the same file, which already use flexible regex matching for invalid regex expression errors.

Fixes #issue

## What type of PR is this:

- [x] BugFix
- [ ] Feature
- [ ] Enhancement
- [ ] Refactor
- [ ] UT
- [ ] Doc
- [ ] Tool

Does this PR entail a change in behavior?

- [ ] Yes, this PR will result in a change in behavior.
- [x] No, this PR will not result in a change in behavior.

If yes, please specify the type of change:

- [ ] Interface/UI changes: syntax, type conversion, expression evaluation, display information
- [ ] Parameter changes: default values, similar parameters but with different default values
- [ ] Policy changes: use new policy to replace old one, functionality automatically enabled
- [ ] Feature removed
- [ ] Miscellaneous: upgrade & downgrade compatibility, etc.

## Checklist:

- [x] I have added test cases for my bug fix or my new feature
- [ ] This pr needs user documentation (for new or modified features or behaviors)
  - [ ] I have added documentation for my new feature or new function
  - [ ] This pr needs auto generate documentation
- [ ] This is a backport pr

## Bugfix cherry-pick branch check:
- [ ] I have checked the version labels which the pr will be auto-backported to the target branch
  - [ ] 4.1
  - [ ] 4.0
  - [ ] 3.5
  - [ ] 3.4
