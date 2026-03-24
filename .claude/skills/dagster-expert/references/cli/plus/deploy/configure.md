---
title: "dg plus deploy configure"
triggers:
  - "Deploying to Dagster Plus, GitHub Actions"
  - "CI/CD configuration"
---

The `dg plus deploy configure` command will scaffold all the necessary files to allow a git repo to be deployed to Dagster Plus. At minimum, this will create a GitHub Actions workflow file, which will automatically handle redeploying to Dagster Plus when commits are merged into the main branch / creating branch deployments for pull requests.

While the command does provide flags / subcommands for specific use cases, invoking it bare is _HIGHLY_ recommended, as this will prompt you for all necessary information, including authenticating with GitHub, as well as configuring container registry credentials if necessary (hybrid deployments).

`dg plus login` must ALWAYS be executed before running this command, which requires that the user create a Dagster Plus account.

**IMPORTANT** In order to successfully deploy to Dagster Plus, the `dagster-cloud` python package must be added as a dependency to the project (e.g. `uv add dagster-cloud`).

```bash
dg plus deploy configure
```
