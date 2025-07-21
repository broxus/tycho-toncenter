## TON Center API for Tycho

A partial TON Center API V2 and V3 implementation for Tycho-based networks. Runs as a single self-contained light node.

## Supported Methods

### API V2

Base route `/toncenter/v2`

* `/` - alias for `/jsonRPC`
* `/jsonRPC` - only methods that are listed here are supported
* `/getMasterchainInfo`
* `/getBlockHeader`
* `/shards`
* `/detectAddress`
* `/getAddressInformation`
* `/getExtendedAddressInformation`
* `/getWalletInformation`
* `/getTokenData`
* `/getTransactions`
* `/getBlockTransactions`
* `/getBlockTransactionsExt`
* `/sendBoc`
* `/sendBocReturnHash`
* `/runGetMethod`

### API V3

Base route `/toncenter/v3`

* `/masterchainInfo`
* `/blocks`
* `/transactions`
* `/transactionsByMasterchainBlock`
* `/adjacentTransactions`
* `/transactionsByMessage`
* `/jetton/masters`
* `/jetton/wallets`

## Install

### Docker images

Docker images are available on Docker Hub.

```bash
# Create persistent folder
mkdir -p data

alias tycho-toncenter='docker run --userns=keep-id \
    --mount type=bind,src=./data,dst=/tycho,z \
    rexagon/tycho-toncenter:latest'

# Generate config.
# NOTE: Paths are relative to the `./data` folder.
tycho-toncenter run --init-config ./config.json

# Download `global-config.json` for the desired network,
# e.g. for the testnet:
wget -O ./data/global-config.json https://testnet.tychoprotocol.com/global-config.json

# Start the node
tycho-toncenter run --config config.json \
    --global-config global-config.json \
    --keys keys.json
```

### Building from source

To build the node from source code, You need:
* Rust: Version specified in [Cargo.toml](./Cargo.toml?#L6) or greater.
* OpenSSL, Zstd, Clang

Install dependencies:
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install build dependencies
sudo apt install build-essential git libssl-dev zlib1g-dev pkg-config clang
```

Install the node:
```bash
git clone https://github.com/broxus/tycho-toncenter
cd tycho-toncenter
cargo install --locked --path .
```

Generate the default node config:
```bash
tycho-toncenter run --init-config ./config.json
```

An example of the above configuration file can be found [here](./examples/config.json).

Run the service:
```bash
# Download `global-config.json` for the desired network,
# e.g. for the testnet:
wget -O ./global-config.json https://testnet.tychoprotocol.com/global-config.json

# And start the node
tycho-toncenter run --config ./config.json --global-config.json --keys keys.json
```

> NOTE: `keys.json` will be generated if not exists.

### Building the Docker image

You can build a docker image locally with the following commands:
```bash
docker build . -t tycho-toncenter:latest
```

## Contributing

We welcome contributions to the project! If you notice any issues or errors,
feel free to open an issue or submit a pull request.

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)
  or <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT)
  or <https://opensource.org/licenses/MIT>)

at your option.
