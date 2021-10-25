## Backtest Hub

__Backtest Hub__ provides a resourceful toolkit to run backtest simulations __quick and easy__, be it:

1) Stock-like 
2) Futures-like, 
3) Rates-like,
4) Multi stock-like
5) Portfolio-like 

Different from other backtest libs I've seen and worked with, backtesthub focuses on it's core mission, which is to provide users with a simple and efficient backtest engine, letting to users the decision to pick sattelites systems (such as ETL/Optimization/Analytics) that work better for their own goals. The project targets professional quantitative/systematic traders and developers, capable to understand how the framework works, and adapt it for its own needs.

This project was inspired mainly by Backtrader and Backtesting.py Libraries... From the first, I got the idea of implementing `Lines` and `Line Buffers` [but be careful, because my implementation difers in various point from his], while from the latter, I got the inspiration on how to integrate `Numpy` and how to leverage it to derive indicators and trading signals definitions. However, Backtesthub has a lot of utils and features that aren't present on neither projects, such as `Pipelines`, `Broadcasting` methods and many more.

This project is still under improvement, since it lacks a lot of features to simulate a real robust trading system. It makes a lot of simplyfing assumptions to make its logic easier [read `Broker` to know more about it], and requires the definition of a bunch of environment variables - but don't worry, most of them already have a very reasonable default value, those do not require manual setup by the user. One special exception are `Database settings`, which are a must in order for the algorithm to have access to data to run its backtests, and to return to the database the results of its calculations (check in `examples` how tables and schemas are defined). 

#### References 

* https://github.com/mementum/backtrader
* https://github.com/kernc/backtesting.py
