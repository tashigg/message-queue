# Ensure that SSH connections to Github authenticate successfully.
if [[ ! -S $SSH_AUTH_SOCK ]]; then
  echo 'Error: Docker build command must be run with `--ssh default` to use ssh mounts.'
  exit 1
fi

# Ensure that `ssh-agent` is running and we have access to its identities.
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

#
if [[ $? -ne 1 ]]; then
  echo 'Error: failed to connect to Github SSH server. Review the output of the previous command for details.'
  exit 1
fi
