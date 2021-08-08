## Backtest Hub

__Backtest Hub__ provides a resourceful toolkit to run backtest simulations __quick and easy__, be it:

1) Single or Multi-asset, 
2) Stock-like or Futures-like, 
3) Price-like or Rates-like, or even 
4) Single-stock-like or Multi-stock-like 

Instead of providing multiple accessory features, such as etl/analytics/optimizing, backtest-hub focuses on it's core mission, which is to provide users with an efficient backtest engine, letting to the user the decision of which sattelites systems fits better for their own goals. The project targets professional quantitative/systematic traders and developers, but, literally, anyone interested in it is welcome to try, support and criticize (constructively, I hope)!

The purpose of the project is to open source the development of a more advanced framework to compute backtests and portfolios than the ones obtained by similar projects such as backtrader, backtesting.py and zipline. 

From Backtrader, I took inspiration of the concept of _Line Data Classes_, which are brilliant, except by the fact that they are very slow when calculated without preloading... but even when preloaded, i.e. calculated vectorized way, the program crashes when too much data is ingested in the core engine, which happens very often in multi-stocklike vectorized runs. 

So I started to search about what were the backtest projects the community have already developed on top of numpy, and the project backtesting.py got my attention by its effectiveness and simplicity. The problem I felt was that this project is very focused on _"optimization"_ (you know, I'm not a big fan of optimizing things in low-frequency quant finance) and narrowly scoped (i.e. no support for future-like simulations, nor multi-stocklike, etc.).

That's why I decided to develop my own backtest engine to be able to overcome those difficulties. This project is still "in progress"... The core code I'm currently using to run the fund's backtests is confidential and won't be published here, but I'm working hard to make this project available ASAP =)

#### References 

* https://github.com/mementum/backtrader
* https://github.com/kernc/backtesting.py
