# This script tries to ensure that SSH connections to Github can authenticate successfully,
# so Cargo can clone `tashi-consensus-engine`. Used for building the project in Docker.
#
# Without these checks, it can be really hard to debug issues with cloning because you'll just always get a generic
# error about "invalid public key".

# Check that `$SSH_AUTH_SOCK` was actually injected into the container.
# Docker has this annoying behavior where if you don't pass `--ssh`, it'll set the environment variable,
# but not actually mount the socket, and everything that depends on this fails silently as well! Very fun.
if [[ ! -S $SSH_AUTH_SOCK ]]; then
  echo 'Error: $SSH_AUTH_SOCK does not exist. If building in Docker, build command must be run with `--ssh default` to use ssh mounts.'
  exit 1
fi

# Ensure that `ssh-agent` is running and we have access to its identities.
# Git doesn't consider it an error not to be able to connect to `ssh-agent`,
# so any problems with it will cause clones to fail with misleading errors like "invalid public key".
ssh-add -l
result=$?

if [[ $result -eq 2 ]]; then
  echo 'Error: ssh-agent is not running on the host machine; run the command `eval $(ssh-agent -s)` to start it';
  exit 1;
elif [[ $result -eq 1 ]]; then
  echo 'Error: ssh-agent is running but does not know any SSH keys; run `ssh-add` with the path to your SSH key (e.g. `ssh-add ~/.ssh/id_ed25519`)';
  exit 1;
fi

# Try to connect to Github's SSH server; if something goes wrong, the output of this command should help explain it.
ssh -v git@github.com

# It's technically going to fail because Github just returns a message and immediately closes the connection,
# but it's at least a specific error code we can test for. Anything else would be 255.
if [[ $? -ne 1 ]]; then
  echo 'Error: failed to connect to Github SSH server. Review the output of the previous command for details.'
  exit 1
fi
