# Githooks
This folder contains an optional [githook](https://git-scm.com/book/en/v2/Customizing-Git-Git-Hooks) to run test and linting before any commit.

# Installation

Copy the `pre-commit` which is included in this folder file into your `*gowarcserver_root*/.git/hooks/` folder

To test that the git hook is properly installed simply run a `git commit` without any staged files. You should see a message like `Running pre-commit hook`, followed by the `go test` command output. The linter should not print anything as long as there are no complaints.