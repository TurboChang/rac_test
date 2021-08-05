# rac_test

### 框架tree
```angular2html
|____core
| |____dbs
| | |____db_oracle.py
| | |______init__.py
| |______init__.py
| |____logics
| | |____db_param.py
| | |____db_driver.py
| | |______init__.py
| | |____db_factory.py
| |____conf
| | |____TestPlan.xlsx
| | |______init__.py
| |____exception
| | |______init__.py
| | |____related_exception.py
|____README.md
|____runner.py
```

### 文件夹结构介绍
```angular2html
assets:                 测试资产文件，包括：测试计划文件，测试数据文件，等其他数据文件
core:                   框架核心
core.conf.TestPlan:     用来接收增量的Oracle数据库(配置excel)
core.dbs:               Oracle基础类(连接池/执行)
core.exception:         异常类
core.compare:           对比Oracle数据源与目的地测试表的数据差异的行
core.report:            记录Oracle数据源与目的地测试表的数据差异的行
core.logics.db_driver:  装饰器
core.logics.db_param:   Faker数据以及解析测试计划(excel)
core.logics.db_factory: 增量数据写入/数据清除
runner:                 框架执行脚本
```

### runner.py使用说明
```angular2html
DataPipeline Runner 用法 通过指定以下参数执行自动化增量数据写入

optional arguments:

  -h, --help            show this help message and exit
  --ops [{trunc,insert}]
                        param : 执行指定操作(写入/清空数据). (default: None)
  --db DB, -d DB        param : 执行指定测试数据库. (default: Oracle)
  --tab TAB, -t TAB     param : 执行指定的测试表. (default: rac_test)
  --batch BATCH, -b BATCH
                        param : 执行指定BATCH大小. (default: 2000)

```

