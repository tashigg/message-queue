# message-queue
Message queue MeshApp utilizing Tashi Consensus Engine

### Checking Out

When checking out, be sure to initialize and update submodules:

```
$ git submodule update --init --recursive
```

The `rumqtt` submodule points to a fork in our organization, so we can make and commit modifications as needed.
GitHub Actions is enabled on the repository. Any value-added changes can be upstreamed for some community goodwill.
https://github.com/tashigg/rumqtt

(`git subtree` is gross.)

### Generating a config file

Use the `address-book` command, which supports various modes. One example is using a port range:
```bash
cargo run -- address-book from-range 127.0.0.1 1883 1884
cat dmq/address-book.toml
```

### Manually generating keys
Using openssl:
```bash
openssl ecparam -name prime256v1 -genkey -noout -out secret.pem
openssl ec -in secret.pem -pubout -out public.pem
```
