#!/usr/bin/env bash

set -e

NUM_REPLICAS=$1

if [[ -z $NUM_REPLICAS ]]; then
cat << 'EOF'
Usage: ./gen-secrets.sh <number of replicas> [--update] [kubectl args..]

Create P-256 keypairs and user credentials for each replica in `0 .. <number of replicas>`.

Creates two Secrets and two ConfigMaps in the current Kubernetes context and default namespace (unless otherwise specified).

If `--update` is specified, this script will overwrite the existing items using `kubectl apply`.
Otherwise, this script will fail.

Each Secret or ConfigMap contains the same number of keys for the given number of replicas,
with the suffix `-N` where N is the zero-based index of the replica.

* Secret: foxmq-demo-secret-keys

PEM-encoded P-256 private keys for the given number of replicas as `key-N.pem`.

* ConfigMap: foxmq-demo-public-keys

PEM-encoded P-256 public keys for the given number of replicas as `key-N.pem`.

* Secret: foxmq-demo-user-credentials

A unique pair of user credentials for each replica keyed as `demo-N: "<password>"`.

* ConfigMap: foxmq-demo-users

Users TOML files for each replica as `users-N.toml`.
EOF
  exit 0
fi

# Pop `$NUM_REPLICAS`
shift 1

UPDATE=false

if [[ "$1" = "--update" ]]; then
  UPDATE=true
  shift 1
fi

PASSWORD_LEN=16

read -r SPECIAL_CHARS <<'END'
!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
END

# Works in backticks but not $()
# shellcheck disable=SC2006
PASSWORD_CHARS="`echo -n {a..z} {A..Z} {0..9} | tr -d ' '`$SPECIAL_CHARS"

function cleanup {
  rm -rf .foxmq.tmp/
}

trap cleanup EXIT

mkdir -p .foxmq.tmp/secret-keys/ .foxmq.tmp/public-keys/ .foxmq.tmp/users/ .foxmq.tmp/credentials

for (( i=0; i<NUM_REPLICAS; i++ ))
do
  openssl ecparam -name prime256v1 -genkey -noout -out .foxmq.tmp/secret-keys/key-$i.pem
  openssl ec -in .foxmq.tmp/secret-keys/key-$i.pem -pubout -out .foxmq.tmp/public-keys/key-$i.pem 2>/dev/null
done

for (( i=0; i<NUM_REPLICAS; i++ ))
do
  # Write the password (without the trailing newline) to a file
  echo -n "$PASSWORD_CHARS" | fold -w1 | shuf -r -n$PASSWORD_LEN | tr -d '\n' > .foxmq.tmp/credentials/demo-$i
  cargo run --manifest-path=../../Cargo.toml -- user generate demo-$i \
    < .foxmq.tmp/credentials/demo-$i \
    > .foxmq.tmp/users/demo-$i.toml
done

if [[ "$UPDATE" = true ]]; then
  # shellcheck disable=SC2068
  kubectl create secret generic foxmq-demo-secret-keys $@ --dry-run=client --save-config -o yaml \
    --from-file=.foxmq.tmp/secret-keys/ | tee | kubectl apply -f -

  # shellcheck disable=SC2068
  kubectl create configmap foxmq-demo-public-keys $@ --dry-run=client --save-config -o yaml \
    --from-file=.foxmq.tmp/public-keys/ | tee | kubectl apply -f -

  # shellcheck disable=SC2068
  kubectl create secret generic foxmq-demo-user-credentials $@ --dry-run=client --save-config -o yaml \
    --from-file=.foxmq.tmp/credentials/ | tee | kubectl apply -f -

  # shellcheck disable=SC2068
  kubectl create configmap foxmq-demo-users $@ --dry-run=client --save-config -o yaml \
    --from-file=.foxmq.tmp/users/ | tee | kubectl apply -f -
else
    # shellcheck disable=SC2068
    kubectl create secret generic foxmq-demo-secret-keys $@ --save-config --from-file=.foxmq.tmp/secret-keys/

    # shellcheck disable=SC2068
    kubectl create configmap foxmq-demo-public-keys $@ --save-config --from-file=.foxmq.tmp/public-keys/

    # shellcheck disable=SC2068
    kubectl create secret generic foxmq-demo-user-credentials $@ --save-config --from-file=.foxmq.tmp/credentials/

    # shellcheck disable=SC2068
    kubectl create configmap foxmq-demo-users $@ --save-config --from-file=.foxmq.tmp/users/
fi
