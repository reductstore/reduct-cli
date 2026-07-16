# ReductStore CLI


[![Crates.io(latest version)](https://img.shields.io/crates/dv/reduct-cli)](https://crates.io/crates/reduct-cli)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reduct-cli/total)](https://github.com/reductstore/reduct-cli/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-cli/ci.yml?branch=main)](https://github.com/reductstore/reduct-cli/actions)


The ReductStore CLI is a command line client for [ReductStore](https://www.reduct.store), a time series database for
blob data.

## Features

* Support for ReductStore API v1.21
* Easy management of buckets, tokens, replications and lifecycle policies
* Ability to check the status of a storage engine
* Aliases for storing server credentials
* Export and mirror data

## Installing

### From Cargo

```shell
cargo install --locked reduct-cli
```

### From Snap

```bash
sudo snap install reduct-cli
```

### From pre-built binaries

You can also install `reduct-cli` from the latest release binaries.

#### Linux (amd64)

```bash
wget https://github.com/reductstore/reduct-cli/releases/latest/download/reduct-cli.x86_64-unknown-linux-gnu.tar.gz
tar -xvf reduct-cli.x86_64-unknown-linux-gnu.tar.gz
chmod +x reduct-cli
sudo mv reduct-cli /usr/local/bin
```

#### Windows (amd64)

```powershell
Invoke-WebRequest -Uri https://github.com/reductstore/reduct-cli/releases/latest/download/reduct-cli.x86_64-pc-windows-gnu.zip -OutFile reduct-cli.zip
Expand-Archive -LiteralPath reduct-cli.zip -DestinationPath .
.\reduct-cli.exe
```

We support many additional platforms. Download binaries and sources from the latest release: [GitHub Releases](https://github.com/reductstore/reduct-cli/releases/latest)

## Usage

Check with our [demo server](https://play.reduct.store):

```shell
reduct-cli alias add play -L  https://play.reduct.store/replica -t reductstore
reduct-cli server status play
reduct-cli bucket ls --full play
reduct-cli cp play/datasets ./datasets --limit 100
reduct-cli cp play/* backup
```

For more examples, see the [Guides](https://www.reduct.store/docs/guides) section in the ReductStore documentation.

## Community & Contribution

If you've found a bug, have ideas, built something with ReductStore CLI, or want to contribute directly, here are the best
places to jump in:

* **Questions and Ideas**: Join our [**Discourse community**](https://community.reduct.store) to ask questions, share ideas,
  and collaborate with fellow ReductStore users.
* **Bug Reports**: Open an issue on our **[GitHub repository](https://github.com/reductstore/reduct-cli/issues)**. Please
  include the ReductStore CLI version, ReductStore server version, the command you ran, and the output or error message.
* **First Contributions**: Pick a task from
  [**good first issues**](https://github.com/reductstore/reduct-cli/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22)
  or [**help wanted**](https://github.com/reductstore/reduct-cli/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22help%20wanted%22).
* **Pull Requests**: Open PRs only for an existing issue. Comment on the issue and get assigned before you start work. Use
  the repository pull request template when you open the PR, and update `CHANGELOG.md` for user-visible changes. If you
  want to propose a new feature or workflow, open an issue first or start the discussion on the
  [**community forum**](https://community.reduct.store) before writing code.
* **Show Your Work**: Share your projects, benchmarks, and lessons learned on our
  [**Discourse community**](https://community.reduct.store).
* **Support the Project**: If ReductStore CLI is useful to you, give us a ⭐ on GitHub.

## Contributors

Thanks to everyone who has contributed to ReductStore CLI.

<p align="center">
  <a href="https://github.com/atimin"><img src="https://avatars.githubusercontent.com/u/67068?v=4" width="48" height="48" alt="@atimin" /></a>
  <a href="https://github.com/AnthonyCvn"><img src="https://avatars.githubusercontent.com/u/26444489?v=4" width="48" height="48" alt="@AnthonyCvn" /></a>
  <a href="https://github.com/szabgab"><img src="https://avatars.githubusercontent.com/u/48833?v=4" width="48" height="48" alt="@szabgab" /></a>
  <a href="https://github.com/vbmade2000"><img src="https://avatars.githubusercontent.com/u/1904995?v=4" width="48" height="48" alt="@vbmade2000" /></a>
  <a href="https://github.com/Rumpelshtinskiy"><img src="https://avatars.githubusercontent.com/u/12251146?v=4" width="48" height="48" alt="@Rumpelshtinskiy" /></a>
  <a href="https://github.com/ychampion"><img src="https://avatars.githubusercontent.com/u/68075205?v=4" width="48" height="48" alt="@ychampion" /></a>
  <a href="https://github.com/yaslam-dev"><img src="https://avatars.githubusercontent.com/u/26070254?v=4" width="48" height="48" alt="@yaslam-dev" /></a>
</p>

## Links

* [Project Homepage](https://www.reduct.store)
* [ReductStore Client SDK for Rust](https://github.com/reductstore/reduct-rs)
* [ReductStore](https://github.com/reductstore/reductstore)
