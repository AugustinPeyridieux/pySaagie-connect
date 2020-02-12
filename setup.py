from setuptools import setup

setup(
    name='pySaagie-connect',
    version='0.1',
    description='Python package to connect to several datalake services (Impala, Hive, HDFS etc..)',
    url='git@github.com:AugustinPeyridieux/pySaagie-connect.git',
    author='Saagie Service team',
    license='GLWTPL',
    packages=['pySaagie-connect'],
    install_requires=[
          'ibis',
          'hdfs',
      ],
    zip_safe=False
)
