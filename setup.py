from sys import version_info
from os.path import join, dirname

if version_info < (3, 6):
    raise Exception('VERSION ERROR: Python 3.6+ required')

def read(fname:str) -> str:
    
    dir = dirname(__file__)
    
    return open(
        join(dir, fname), 
        encoding='utf-8'
    ).read()

if __name__ == '__main__':
    from setuptools import setup, find_packages

    setup(
        name = "backtesthub",
        license = 'AGPL-3.0',
        author = "Matheus Pires",
        author_email = "matheus.augusto.pires@alumni.usp.br",
        description = "BacktestHub is an efficient and simple "
        "python-based backtest framework",
        long_description = read('README.md'),
        packages = find_packages(),
        version = '4.0.0',
        
    )