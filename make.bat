c:\PORT-STC\opt\Pandoc\pandoc -r rst README.rst -o readme.html
python setup-wheel.py clean bdist_wheel
rmdir /q /s build
rmdir /q /s pyxfer.egg-info

REM pip install --upgrade --no-deps --force-reinstall dist\pyxfer-0.1.0-py3-none-any.whl
