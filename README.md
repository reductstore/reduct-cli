# ReductStore CLI


[![Crates.io(latest version)](https://img.shields.io/crates/dv/reduct-cli)](https://crates.io/crates/reduct-cli)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reduct-cli/total)](https://github.com/reductstore/reduct-cli/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-cli/ci.yml?branch=main)](https://github.com/reductstore/reduct-cli/actions)


The ReductStore CLI is a command line client for [ReductStore](https://www.reduct.store), a time series database for
blob data.

## Features

* Support for ReductStore API v1.18
* Easy management of buckets, tokens and replications
* Ability to check the status of a storage engine
* Aliases for storing server credentials
* Export and mirror data

## Installing

### From Cargo

```shell
cargo install reduct-cli
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


## Links

* [Project Homepage](https://www.reduct.store)
* [ReductStore Client SDK for Rust](https://github.com/reductstore/reduct-rs)
* [ReductStore](https://github.com/reductstore/reductstore)
