from setuptools import setup, Extension
from Cython.Build import cythonize

# List of Cython modules to compile
extensions = [
    Extension("private_key_generator", ["private_key_generator.pyx"]),
    Extension("database_operations", ["database_operations.pyx"]),
]

setup(
    ext_modules = cythonize(extensions),
)
