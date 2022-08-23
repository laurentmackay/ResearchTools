import sys
from platform import uname

from setuptools import setup, find_packages

IS_LINUX = sys.platform.startswith('linux')
IS_WINDOWS = sys.platform.startswith('win')


install_requires=['numpy>1.10', 'psutil', 'scipy>=0.17.0', 'numba>=0.54']
dependency_links =[]


if IS_WINDOWS:
	install_requires.append('dill')
	dependency_links.append('git+https://github.com/uqfoundation/dill.git@master')

install_requires.append('pathos')
dependency_links.append('git+https://github.com/uqfoundation/pathos.git@master')

setup(name='ResearchTools',
      version='1.0',
      packages=find_packages(),
      install_requires=install_requires,
      dependency_links=dependency_links
      )