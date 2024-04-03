# ReductStore CLI


[![Crates.io(latest version)](https://img.shields.io/crates/dv/reduct-cli)](https://crates.io/crates/reduct-cli)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reduct-cli/total)](https://github.com/reductstore/reduct-cli/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-cli/ci.yml?branch=main)](https://github.com/reductstore/reduct-cli/actions)


The ReductStore CLI is a command line client for [ReductStore](https://www.reduct.store), a time series database for
blob data.

## Features

* Support for ReductStore API v1.9
* Easy management of buckets, tokens and replications
* Ability to check the status of a storage engine
* Aliases for storing server credentials
* Export and mirror data

## Installing

### Cargo

```shell
cargo install reduct-cli
```

### Binary Linux

```shell
wget https://github.com/reductstore/reduct-cli/releases/latest/download/reduct-cli.linux-amd64.tar.gz
tar -xvf reduct-cli.linux-amd64.tar.gz
chmod +x reduct-cli
sudo mv reduct-cli /usr/local/bin
```

### Binary MacOS

```shell
wget https://github.com/reductstore/reduct-cli/releases/latest/download/reduct-cli.macos-amd64.tar.gz
tar -xvf reduct-cli.macos-amd64.tar.gz
chmod +x reduct-cli
sudo mv reduct-cli /usr/local/bin
```


### Binary Windows

```powershell
Invoke-WebRequest -Uri  https://github.com/reductstore/reduct-cli/releases/latest/download/reduct-cli.win-amd64.zip -OutFile reductstore.zip
Expand-Archive -LiteralPath reductstore.zip -DestinationPath .
.\reductstore.exe
```

## Usage

Check with our [demo server](https://play.reduct.store):

```shell
reduct-cli alias add -L  https://play.reduct.store -t reduct play
reduct-cli server status play
reduct-cli bucket ls --full play
reduct-cli cp play/datasets .
```

## Links

* [Project Homepage](https://www.reduct.store)
* [ReductStore Client SDK for Rust](https://github.com/reductstore/reduct-rs)
* [ReductStore](https://github.com/reductstore/reductstore)
