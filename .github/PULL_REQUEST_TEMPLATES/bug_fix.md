## 🐛 Bug Fix

### Description

Provide a concise summary of the issue you are fixing. Link an issue ticket if one exists.

### Root Cause:

Explain the underlying cause of the bug. This helps reviewers understand why the issue occurred and how your fix addresses it.
***The bug is caused by a missing null check in the form validation logic. When the input value is empty, the code attempts to access properties on a null object, leading to the crash.***

### Testing:

Detail the steps you took to test your fix and any additional tests added. If applicable, mention if you ran existing test suites and their results.

### Checklist

- [ ] I have included a clear description of the bug.
- [ ] I have listed the steps to reproduce the issue.
- [ ] I have run the test suite and verified it passes.
- [ ] I have attached any relevant logs or screenshots.

### Who can review?

List any maintainers or contributors who are best suited to review this pull request.
