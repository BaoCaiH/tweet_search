# First run this to update the packages
# Or change to apt, depends
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl

# Then run this to install pyenv
curl https://pyenv.run | bash

# Afterward you should see something like this if it couldn't add the pyenv to PATH
# automatically
# *******************************************************************
# WARNING: seems you still have not added 'pyenv' to the load path.

# Load pyenv automatically by adding
# the following to ~/.bashrc:

# export PATH="$HOME/.pyenv/bin:$PATH"
# eval "$(pyenv init -)"
# eval "$(pyenv virtualenv-init -)"
# *******************************************************************
# Then reset shell and check for pyenv
exec "$SHELL"
pyenv install --list | grep "3.8"

# If you see 3.8.8 there, then:
pyenv install 3.8.8
# Verify
pyenv versions
pyenv global 3.8.8

# At this point I think it's should be fine
# The last command might only work for your user, when I login I might have
# to do that again, I've never work with multiple users like this before.
# but the important outcome should be a python 3.8 installed somewhere
# in the machine, whether it's a pyenv version or a flat python installation from source.

# Thank you
