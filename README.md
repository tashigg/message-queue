# foxmq
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
cat foxmq.d/address-book.toml
```

### Manually generating keys
Using openssl:
```bash
openssl ecparam -name prime256v1 -genkey -noout -out secret.pem
openssl ec -in secret.pem -pubout -out public.pem
```

### Fuzzing (currently only possible on non-windows systems)
> Note: libFuzzer needs LLVM sanitizer support, so this only works on x86-64 Linux,
> x86-64 macOS and Apple-Silicon (aarch64) macOS for now. You'll also need a C++ compiler with C++11 support.
> Install `cargo-fuzz` and the nightly toolchain:
```
cargo install cargo-fuzz
rustup toolchain install nightly
rustup +nightly component add llvm-tools-preview
```
#### Run fuzzing commands
note:
you'll probably want to build the appropriate corpuses by starting at small max sizes ƒor a short amount of time,
then move up by powers of two until you reach the size you want to leave off at.
This helps the fuzzer build a more compact corpus which leads to greater fuzzing efficiency (and that multiplies out over a long period of time)

You can find the reference for the dictionary format in the libFuzzer documentation: https://llvm.org/docs/LibFuzzer.html#dictionaries

```sh
# running fuzz on the `trie` target (from the project root). 
# `dict` specifies a dictionary, for things like magic strings or keywords. 
# `max_total_time` sets a time limit for the entire fuzzing operation, (useful for building a corpus), 
# if set to `0` or omitted there's no timelimit.
# `max_len` specifies the maximum length of fuzzer provided inputs to the target, the default is `4096` as shown here.
# `max_len` effectively controls the size of the search space,
# smaller means more of it will get visited, 
# but larger means there is more space for complex interesting behaviors (crashes/timeouts are the interesting things).
# By default the target is built in release mode with debug-assertions enabled.
cargo +nightly fuzz run trie -- -dict=fuzz/dicts/trie -max_total_time=0 -max_len=4096

# fuzz rumqtt protocol v4 parsing.
# see above for descriptions of arguments.
cargo +nightly fuzz run rumqtt_proto_v4 -- -dict=fuzz/dicts/rumqtt_proto_v4

# fuzz rumqtt protocol v5 parsing.
# see above for descriptions of arguments.
cargo +nightly fuzz run rumqtt_proto_v5 -- -dict=fuzz/dicts/rumqtt_proto_v5

# fuzz `ClientId` parsing
# see above for descriptions of arguments.
cargo +nightly fuzz run client_id -- -dict=fuzz/dicts/client_id
```
