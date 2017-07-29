python3=python3.6
venv_dir=venv

run: compute_errors.py
	$(venv_dir)/bin/python3 compute_errors.py

$(venv_dir)/packages-installed: requirements.txt
	test -d $(venv_dir) || $(python3) -m venv $(venv_dir)
	$(venv_dir)/bin/pip install -U pip wheel
	$(venv_dir)/bin/pip install cython
	$(venv_dir)/bin/pip install -r requirements.txt
	touch $@
