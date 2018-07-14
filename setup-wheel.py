"""
To build a Wheel : python tools/server-wheel.py clean bdist_wheel
--> wheel will be put under the ./dist directory

To build a source package (zip file) : python tools/server-wheel.py clean sdist (note the directory position)

The wheel will be put in tools\dist\KoiServer-0.1.0-py3-none-any.whl

To test the wheel :

wheel install tools\dist\KoiServer-0.1.0-py3-none-any.whl # Doesn't work on virtualenv I think

To install on virtual env :
---------------------------

Enter virtualenv :

# In, C:\PORT-STC\PRIVATE\PL\Koi :

C:\PORT-STC\PRIVATE\PL\venv\Scripts\activate.bat

pip list # to see what's installed

# If you need dependencies not on  PyPI :
# On Windows, the following package won't be installed because they require
# compilation : lxml, psycopg2.

pip install c:\ Users\stc\Downloads\psycopg2-2.6.1-cp34-none-win32.whl
pip install c:\ Users\stc\Downloads\lxml-3.4.4-cp34-none-win32.whl


On Linux, pymediafire requests lxml. Since lxml compilation is a bit tricky,
use the python3-lxml debian package (instead of pip). So inistall that first,
then pymediafire next.



pip install Koi\dist\KoiServer-0.1.0-py3-none-any.whl

# Clean install of the Koi server (dependencies should be downloaded automatically by pip)
pip uninstall KoiServer # guess what
pip install dist\KoiServer-0.1.0-py3-none-any.whl


# Test it
python -m src.server.cherry

# Leave virtual env
deactivate

"""




import os
import setuptools


# BASE_DIR = os.path.join( os.getcwd(), os.path.dirname(__file__))

def readme():
    with open('README.rst') as f:
        return f.read()
# print("Packages will be looked for in {}".format(BASE_DIR))

# p = setuptools.find_packages(BASE_DIR, include=[ "montgomery"])
# print(p)

# for the classifiers and metadata, see : https://packaging.python.org/
setuptools.setup(
    name= "montgomery",
    description="A serializer code generator (based on SQLAlchemy)",
    long_description=readme(),
    license="GNU Lesser General Public License v3 or later (LGPLv3+)",
    version="0.1.0",
    url="www.koi.org",
    author="Stefan Champailler",
    author_email="schampailler@skynet.be",
    python_requires='~=3.6',
    packages=['montgomery'],
    install_requires=['sqlalchemy>=1.0.0, <1.1.0'],
    classifiers=['Programming Language :: Python :: 3.6',
                 'Development Status :: 3 - Alpha',
                 'Topic :: Software Development :: Code Generators']
)
