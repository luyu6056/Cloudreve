## 第一版优化内容：

 * webdav备注名 “只读”，这个密码下的webdav，只允许在“提交”目录里面增删改，其他目录均为只读功能
 * chan异步打印，util.Log()里面打印的内容不再加锁进行实时打印（减少1-2ms响应时间)
 * 增加models下user,webdav,floder缓存设计，减少mysql查询次数
 * 针对raidrive，拦截0kb的文件写入

## 第二版优化内容：

 * 优化models下user,webdav,floder缓存设计，再次减少mysql查询次数
 * 针对slave启动h2c服务器，复用h2c或者http2客户端，减少master对slave通信的延迟
 * 删除其他数据库驱动，使用我自己的mysql驱动针对本项目适配，减少mysql请求时间
