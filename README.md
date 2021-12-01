Backtesthub
======================

Backtesthub is a simple and efficient backtest framework, built upon numpy and pandas to enable fast calculations, providing handy resources for quantitative researchers: 

1) Run backtests on signal based (systematic) strategies; 
2) Manage position/sizing [stocks and futures]; 
3) Manage orders w/ commission & slippage schemes; 
4) Output backtest metrics for evaluation.

This project attempts to merge ideas from the two best python backtest frameworks _imo_ [[backtrader](https://www.backtrader.com/) and [backtesting](https://github.com/nashquant/backtesting.py)], and introduces some ideas I think could've implemented on both (e.g. broadcasting, base/asset separation, etc.)

Note: The project is still going under several improvements. Do not use it without reading the comments on code!! - Important simplyfing assumptions and details on inner workings are explained in detail.

## Setup

Conda
# Create a new environment
conda create -name backtest
source activate backtest
# Install libraries needed to run code within environment
pip install -r requirements.txt
# Run setup.py and you should be good to go!
python setup.py install

