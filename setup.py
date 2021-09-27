import os

def read(fname:str) -> str:
    dir = os.path.dirname(__file__)
    
    return open(
        os.path.join(dir, fname), 
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
        packages = find_packages(include=['backtesthub', 'backtesthub.*']),
        version = '4.0.0',
    )