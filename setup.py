# Adapted from the pyzmq and pandas setup.py
# Thanks to Wes & MinRK

import os
import shutil
from os.path import abspath, basename, join as pjoin

from distutils.core import setup, Command
from distutils.extension import Extension
from distutils.sysconfig import get_python_inc, get_config_var

try:
    import nose
except ImportError:
    nose = None

interp  = basename(get_python_inc())
prefix  = get_config_var("prefix")
include = get_python_inc(prefix = prefix)

# Yeah, it's ugly. Whatever. There's just no good way to get the site
# headers
site_include = abspath(pjoin(include, '../site', interp))

libraries = []
includes = [site_include]
define_macros = [ ('DEBUG', '1') ]

extensions = [
    Extension(
        name          = "flowlet.flow",
        sources       = ["flowlet/flow.c"],
        include_dirs  = includes,
        libraries     = libraries,
        define_macros = define_macros,
    ),
]

def find_packages():
    packages = []
    for dir,subdirs,files in os.walk('flowlet'):
        package = dir.replace(os.path.sep, '.')
        if '__init__.py' not in files:
            # not a package
            continue
        packages.append(package)
    return packages

class CleanCommand(Command):
    """Custom distutils command to clean the .so and .pyc files."""

    user_options = [ ]

    def initialize_options(self):
        self._clean_me = []
        self._clean_trees = []
        for root, dirs, files in list(os.walk('flowlet')):
            for f in files:
                if os.path.splitext(f)[-1] in ('.pyc', '.so', '.o', '.pyd'):
                    self._clean_me.append(pjoin(root, f))
            for d in dirs:
                if d == '__pycache__':
                    self._clean_trees.append(pjoin(root, d))

        for d in ('build',):
            if os.path.exists(d):
                self._clean_trees.append(d)

    def finalize_options(self):
        pass

    def run(self):
        for clean_me in self._clean_me:
            try:
                os.unlink(clean_me)
            except Exception:
                pass
        for clean_tree in self._clean_trees:
            try:
                shutil.rmtree(clean_tree)
            except Exception:
                pass

#-----------------------------------------------------------------------------
# Main setup
#-----------------------------------------------------------------------------

long_desc = """
Flowlets are a small concurrency structure for building stream pipelines
through coroutines. They can be thought of as "Greenlets with additional
structure" or as a way of making greenlet control flow look more like
Haskell functor composition.
"""

setup(
    name = "flowlet",
    version = '0.0.1dev',
    packages = find_packages(),
    ext_modules = extensions,
    package_data = {},
    author = "Stephen Diehl",
    author_email = "stephen.m.diehl@gmail.com",
    url = 'http://github.com/sdiehl/flowlet',
    download_url = 'http://github.com/sdiehl/flowlet/downloads',
    description = "",
    long_description = long_desc,
    license = "MIT",
    cmdclass = {'clean': CleanCommand},
    #test_suite = "nose.collector",
    classifiers = [
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Mathematics',
        'Topic :: System :: Distributed Computing',
        'Operating System :: POSIX',
        'Topic :: System :: Networking',
        'Programming Language :: C'
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: MIT License'
    ]
)
