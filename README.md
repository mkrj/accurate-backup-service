# 精准数据备份服务

## 使用场景

从多个的业务数据库中按照「组织 ID」备份数据到统一备份数据库中，然后将备份数据库中指定「组织」导出备份文件同步到 OSS

## 特性

1\. 单个组织数据精准备份
- 支持多个源数据库和多个目标数据库，可以多对一备份或者一对一备份
- 源数据库备份支持 join 和 where 筛选条件。
> 注：某些表可能不含类似 「组织 ID」字段，可以通过单表或者多表 join 访问「组织 ID」字段。

2\. 多进程高速备份
- 使用**进程池**异步并发执行数据复制、导出、导入，提高速度

3\. 流式压缩/上传至 OSS
- 数据库导出备份文件采用 `Zstandard` 流式压缩
- 备份文件流式上传 OSS 不落盘，减少对本地磁盘空间依赖

4\.完整的备份日志

## 安装

```shell
pipenv install
```

## 配置和运行

配置文件
 
 ```
 ./udbs/config/db[log][oss].py
 ```
 
 运行
 
 ```shell
 pipenv run backup
 ```


## 注意

- macos 遇到 `+[__NSPlaceholderDate initialize] may have been in progress in another thread when fork() was called.` 错误时，通过设置
环境变量 `export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` 解决
- 保证本地 shell 下可以直接运行 `mysqldump` 命令
- `target_db` 目标数据库不会自动清除重名表，需要手动处理

## 参考：

- https://facebook.github.io/zstd/
- https://github.com/indygreg/python-zstandard
- https://liuyix.github.io/blog/2015/python-compress-stream/
- https://click.palletsprojects.com/en/7.x/
- https://github.com/aliyun/aliyun-oss-python-sdk
- https://www.jianshu.com/p/e62e17c137cb
