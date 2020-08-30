# PQL执行器 - Worker 

**Worker** 是PQL的独立执行器，可以执行PQL过程文件和语句。Worker本身是一个jar包，通过接收各种参数来实现相应的执行功能。

### 项目文件

项目核心只有一个文件，在项目中分别使用 Java 和 Scala 实现。Java文件路径为`io.qross.worker.Main`（仅作参考），Scala文件路径为`io.qross.worker.Worker`，是项目的入口文件。

### 环境要求

* Intellij Idea 2018或以上版本（强烈建议）
* JDK 1.8或以上版本（必须）
* Scala 2.12 或以上版本
* Gradle 4.9 或以上版本（可自行修改成Maven）

如果你不会Java或Scala开发，或者改变环境非常烦琐，可联系作者进行打包。

## Worker参数列表

### --file path.sql
执行PQL文件, path为完整路径, 如 /path/test.sql, 路径中间如果有空格, 需要加双引号, 如 --file "c:/sql files/test.sql"

### --sql "SELECT * FROM table"
执行PQL语句, 因为有空格, 所以需要用引号把语句引起来, 不建议用这种方式执行过长的SQL语句。注意PQL语句中的双引号需要转换成`~u0034`。

### --var name1=value1&b=2&c=hello
PQL语句的入参, 格式同URL地址参数规则, 参数名和参数值之间使用等号`=`分隔, 参数和参数之间使用与号`&`分隔。

### --login userid=1&username=Tom&role=monitor&email=tom@school.com
可以将用户信息传入PQL，在PQL过程中可以全局变量调用这些鉴权信息，如变量`@role`可以得到`monitor`。如果你的PQL过程需要根据不同的用户执行不同的操作时尤其重要。

### --note 24
[Keeper](http://www.qross.cn/keeper/overview) 调度即时查询的命令，如果使用到了[Master](http://www.qross.cn/master/overview)的数据管理和即时查询功能请必须保留。

### --task 23476
[Keeper](http://www.qross.cn/keeper/overview) 调度PQL任务的命令，如果使用Keeper调度你的任务请必须保留。


## 更简单的方式使用Worker

因为Worker是一个jar包，如果你需要在服务器上经常输入命令执行会相当麻烦。可以将Worker修改成一个Shell命令，如
```sh
pql --file /usr/qross/pql/test.sql
```
操作步骤如下：
1. 创建名为`pql.sh`的文件，Qross系统中已经有这个文件。
    ```
    vim pql.sh
    ```
2. 在打开的编辑器中，按`I`键输入下面的命令并按`ESC`然后输入`:wq`保存并退出。
    ```sh
    /usr/java/jdk1.8.0_251/bin/java -jar /usr/qross/qross-worker-0.6.4.jar $*
    ```
    其中java目录根据你的安装确定，使用`which java`命令可以查看。Worker目录修改成Worker jar包的保存位置。
3. 输入`cd /usr/local/bin`切换目录。
4. 在`/usr/local/bin`目录下创建软链接。
    ```sh
    ln -s /usr/qross/pql.sh pql
    ```
5. 完成，输入`pql`命令进行测试。

## 技术支持

**官方网站 [www.qross.io](http://www.qross.io)**  
**作者邮箱 [wu@qross.io](mailto:wu@qross.io)**

---
参考链接

* [PQL概览](http://www.qross.cn/pql/overview)
* [任务调度工具 Keeper](http://www.qross.cn/keeper/overview)
* [数据管理平台 Master](http://www.qross.cn/master/overview)