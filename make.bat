c:\PORT-STC\opt\Pandoc\pandoc -r rst README.rst -o readme.html
python setup-wheel.py clean bdist_wheel
rmdir /q /s build
rmdir /q /s Montgomery.egg-info
